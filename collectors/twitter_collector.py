"""
Twitter/X data collector using Tweepy.
Collects tweets based on keywords, hashtags, or user timelines.
"""

import tweepy
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
from base_collector import BaseCollector
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class TwitterCollector(BaseCollector):
    """Twitter/X data collector."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Twitter collector.

        Args:
            config: Configuration dictionary
        """
        super().__init__(config)
        self.client = self._create_twitter_client()
        self.api = self._create_twitter_api()
        self.keywords = config.get('keywords', ['bitcoin', 'ethereum', 'crypto'])
        self.hashtags = config.get('hashtags', ['#bitcoin', '#ethereum', '#crypto'])
        self.usernames = config.get('usernames', [])
        self.tweet_fields = [
            'id', 'text', 'created_at', 'author_id', 'public_metrics',
            'context_annotations', 'entities', 'geo', 'lang',
            'reply_settings', 'referenced_tweets'
        ]
        self.user_fields = [
            'id', 'name', 'username', 'description', 'public_metrics',
            'verified', 'location', 'created_at'
        ]
        self.expansions = ['author_id', 'referenced_tweets.id']

    def _create_twitter_client(self) -> tweepy.Client:
        """Create and configure Twitter API v2 client."""
        try:
            client = tweepy.Client(
                bearer_token=os.getenv('TWITTER_BEARER_TOKEN'),
                consumer_key=os.getenv('TWITTER_API_KEY'),
                consumer_secret=os.getenv('TWITTER_API_SECRET'),
                access_token=os.getenv('TWITTER_ACCESS_TOKEN'),
                access_token_secret=os.getenv('TWITTER_ACCESS_TOKEN_SECRET'),
                wait_on_rate_limit=True
            )

            # Test connection
            me = client.get_me()
            logger.info(f'Successfully connected to Twitter API as user: {me.data.username}')

            return client

        except Exception as e:
            logger.error(f'Failed to connect to Twitter API: {e}')
            raise

    def _create_twitter_api(self) -> Optional[tweepy.API]:
        """Create Twitter API v1.1 for additional functionality."""
        try:
            if all([
                os.getenv('TWITTER_API_KEY'),
                os.getenv('TWITTER_API_SECRET'),
                os.getenv('TWITTER_ACCESS_TOKEN'),
                os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
            ]):
                auth = tweepy.OAuthHandler(
                    os.getenv('TWITTER_API_KEY'),
                    os.getenv('TWITTER_API_SECRET')
                )
                auth.set_access_token(
                    os.getenv('TWITTER_ACCESS_TOKEN'),
                    os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
                )

                api = tweepy.API(auth, wait_on_rate_limit=True)
                logger.info('Twitter API v1.1 client created successfully')
                return api
            else:
                logger.warning('Missing Twitter API v1.1 credentials, using only v2')
                return None

        except Exception as e:
            logger.warning(f'Failed to create Twitter API v1.1 client: {e}')
            return None

    def collect_data(self) -> List[Dict[str, Any]]:
        """
        Collect tweets based on configured criteria.

        Returns:
            List of collected tweets
        """
        data = []

        try:
            # Collect tweets by keywords
            if self.keywords:
                keyword_tweets = self._collect_by_keywords()
                data.extend(keyword_tweets)

            # Collect tweets by hashtags
            if self.hashtags:
                hashtag_tweets = self._collect_by_hashtags()
                data.extend(hashtag_tweets)

            # Collect tweets from specific users
            if self.usernames:
                user_tweets = self._collect_by_users()
                data.extend(user_tweets)

            logger.debug(f'Collected {len(data)} tweets from Twitter')
            return data

        except Exception as e:
            logger.error(f'Error in Twitter data collection: {e}')
            return []

    def _collect_by_keywords(self) -> List[Dict[str, Any]]:
        """Collect tweets based on keywords."""
        tweets = []

        for keyword in self.keywords:
            try:
                query = f'{keyword} -is:retweet lang:en'

                response = self.client.search_recent_tweets(
                    query=query,
                    max_results=100,
                    tweet_fields=self.tweet_fields,
                    user_fields=self.user_fields,
                    expansions=self.expansions
                )

                if response.data:
                    for tweet in response.data:
                        tweet_data = self._extract_tweet_data(tweet, response.includes)
                        tweets.append(tweet_data)

            except Exception as e:
                logger.error(f'Error collecting tweets for keyword {keyword}: {e}')
                continue

        return tweets

    def _collect_by_hashtags(self) -> List[Dict[str, Any]]:
        """Collect tweets based on hashtags."""
        tweets = []

        for hashtag in self.hashtags:
            try:
                query = f'{hashtag} -is:retweet lang:en'

                response = self.client.search_recent_tweets(
                    query=query,
                    max_results=100,
                    tweet_fields=self.tweet_fields,
                    user_fields=self.user_fields,
                    expansions=self.expansions
                )

                if response.data:
                    for tweet in response.data:
                        tweet_data = self._extract_tweet_data(tweet, response.includes)
                        tweets.append(tweet_data)

            except Exception as e:
                logger.error(f'Error collecting tweets for hashtag {hashtag}: {e}')
                continue

        return tweets

    def _collect_by_users(self) -> List[Dict[str, Any]]:
        """Collect tweets from specific user timelines."""
        tweets = []

        for username in self.usernames:
            try:
                # Get user ID from username
                user = self.client.get_user(username=username)
                if not user.data:
                    logger.warning(f'User {username} not found')
                    continue

                # Get user timeline
                response = self.client.get_users_tweets(
                    id=user.data.id,
                    max_results=100,
                    tweet_fields=self.tweet_fields,
                    user_fields=self.user_fields,
                    expansions=self.expansions,
                    exclude=['retweets']
                )

                if response.data:
                    for tweet in response.data:
                        tweet_data = self._extract_tweet_data(tweet, response.includes)
                        tweets.append(tweet_data)

            except Exception as e:
                logger.error(f'Error collecting tweets for user {username}: {e}')
                continue

        return tweets

    def _extract_tweet_data(self, tweet: Any, includes: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract relevant data from a tweet.

        Args:
            tweet: Tweepy tweet object
            includes: Includes data (users, referenced tweets, etc.)

        Returns:
            Extracted tweet data
        """
        # Get author information
        author = None
        if includes and 'users' in includes:
            for user in includes['users']:
                if user.id == tweet.author_id:
                    author = user
                    break

        # Get referenced tweets (replies, retweets, quotes)
        referenced_tweets = []
        if hasattr(tweet, 'referenced_tweets') and tweet.referenced_tweets:
            for ref_tweet in tweet.referenced_tweets:
                referenced_tweets.append({
                    'type': ref_tweet.type,
                    'id': ref_tweet.id
                })

        return {
            'id': tweet.id,
            'type': 'tweet',
            'text': tweet.text,
            'author_id': tweet.author_id,
            'author': {
                'id': author.id if author else None,
                'name': author.name if author else None,
                'username': author.username if author else None,
                'description': author.description if author else None,
                'verified': author.verified if author else False,
                'followers_count': author.public_metrics['followers_count'] if author and hasattr(author, 'public_metrics') else 0,
                'following_count': author.public_metrics['following_count'] if author and hasattr(author, 'public_metrics') else 0,
                'tweet_count': author.public_metrics['tweet_count'] if author and hasattr(author, 'public_metrics') else 0,
                'listed_count': author.public_metrics['listed_count'] if author and hasattr(author, 'public_metrics') else 0
            },
            'created_at': tweet.created_at.isoformat() if tweet.created_at else None,
            'public_metrics': tweet.public_metrics,
            'lang': tweet.lang,
            'reply_settings': tweet.reply_settings,
            'referenced_tweets': referenced_tweets,
            'context_annotations': tweet.context_annotations if hasattr(tweet, 'context_annotations') else [],
            'entities': tweet.entities if hasattr(tweet, 'entities') else {},
            'geo': tweet.geo if hasattr(tweet, 'geo') else None
        }

    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform Twitter data into standardized format.

        Args:
            raw_data: Raw Twitter data

        Returns:
            Transformed data
        """
        # Extract hashtags and mentions
        hashtags = []
        mentions = []

        if raw_data.get('entities'):
            if 'hashtags' in raw_data['entities']:
                hashtags = [tag['tag'] for tag in raw_data['entities']['hashtags']]
            if 'mentions' in raw_data['entities']:
                mentions = [mention['username'] for mention in raw_data['entities']['mentions']]

        # Extract media information
        media_count = 0
        if raw_data.get('entities') and 'media' in raw_data['entities']:
            media_count = len(raw_data['entities']['media'])

        # Determine tweet type
        tweet_type = 'original'
        if raw_data.get('referenced_tweets'):
            ref_type = raw_data['referenced_tweets'][0]['type']
            if ref_type == 'retweeted':
                tweet_type = 'retweet'
            elif ref_type == 'replied_to':
                tweet_type = 'reply'
            elif ref_type == 'quoted':
                tweet_type = 'quote'

        transformed = {
            'platform': 'twitter',
            'platform_id': str(raw_data['id']),
            'item_type': raw_data['type'],
            'title': '',  # Tweets don't have titles
            'text': raw_data['text'],
            'author': raw_data['author']['username'] if raw_data.get('author') else None,
            'author_id': raw_data.get('author_id'),
            'community': None,  # Twitter doesn't have subreddits
            'score': self._calculate_twitter_score(raw_data),
            'upvote_ratio': None,
            'comment_count': raw_data['public_metrics']['reply_count'] if raw_data.get('public_metrics') else 0,
            'retweet_count': raw_data['public_metrics']['retweet_count'] if raw_data.get('public_metrics') else 0,
            'like_count': raw_data['public_metrics']['like_count'] if raw_data.get('public_metrics') else 0,
            'quote_count': raw_data['public_metrics']['quote_count'] if raw_data.get('public_metrics') else 0,
            'view_count': raw_data['public_metrics'].get('view_count', 0) if raw_data.get('public_metrics') else 0,
            'created_at': raw_data['created_at'],
            'url': f"https://twitter.com/i/web/status/{raw_data['id']}",
            'permalink': f"https://twitter.com/{raw_data.get('author', {}).get('username', 'unknown')}/status/{raw_data['id']}",
            'language': raw_data.get('lang'),
            'text_clean': None,  # Will be filled in preprocessing
            'metadata': {
                'tweet_type': tweet_type,
                'hashtags': hashtags,
                'mentions': mentions,
                'media_count': media_count,
                'context_annotations': raw_data.get('context_annotations', []),
                'geo': raw_data.get('geo'),
                'reply_settings': raw_data.get('reply_settings'),
                'author_verified': raw_data.get('author', {}).get('verified', False),
                'author_followers': raw_data.get('author', {}).get('followers_count', 0),
                'referenced_tweets': raw_data.get('referenced_tweets', [])
            }
        }

        return transformed

    def _calculate_twitter_score(self, tweet_data: Dict[str, Any]) -> float:
        """
        Calculate a unified score for a tweet based on various metrics.

        Args:
            tweet_data: Tweet data

        Returns:
            Calculated score
        """
        if not tweet_data.get('public_metrics'):
            return 0.0

        metrics = tweet_data['public_metrics']

        # Weight different metrics
        score = (
            metrics.get('like_count', 0) * 1.0 +
            metrics.get('retweet_count', 0) * 2.0 +
            metrics.get('reply_count', 0) * 1.5 +
            metrics.get('quote_count', 0) * 1.8
        )

        # Bonus for verified authors
        if tweet_data.get('author', {}).get('verified', False):
            score *= 1.2

        return round(score, 2)