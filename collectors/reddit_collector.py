"""
Reddit data collector using PRAW (Python Reddit API Wrapper).
Collects posts and comments from specified subreddits.
"""

import praw
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from base_collector import BaseCollector
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class RedditCollector(BaseCollector):
    """Reddit data collector."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Reddit collector.

        Args:
            config: Configuration dictionary
        """
        super().__init__(config)
        self.reddit = self._create_reddit_instance()
        self.subreddits = config.get('subreddits', ['cryptocurrency', 'Bitcoin', 'ethereum'])
        self.post_limit = config.get('post_limit', 100)
        self.comment_limit = config.get('comment_limit', 50)
        self.keywords = config.get('keywords', [])
        self.min_score = config.get('min_score', 1)

    def _create_reddit_instance(self) -> praw.Reddit:
        """Create and configure Reddit instance."""
        try:
            reddit = praw.Reddit(
                client_id=os.getenv('REDDIT_CLIENT_ID'),
                client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
                user_agent=os.getenv('REDDIT_USER_AGENT', 'SentimentAnalysis/1.0'),
                read_only=True
            )

            # Test connection
            reddit.read_only

            logger.info('Successfully connected to Reddit API')
            return reddit

        except Exception as e:
            logger.error(f'Failed to connect to Reddit API: {e}')
            raise

    def collect_data(self) -> List[Dict[str, Any]]:
        """
        Collect new posts and comments from specified subreddits.

        Returns:
            List of collected posts and comments
        """
        data = []

        try:
            # Collect posts from each subreddit
            for subreddit_name in self.subreddits:
                try:
                    subreddit = self.reddit.subreddit(subreddit_name)

                    # Get new posts
                    for post in subreddit.new(limit=self.post_limit):
                        if post.score >= self.min_score:
                            # Filter by keywords if specified
                            if self.keywords and not any(
                                keyword.lower() in post.title.lower() or
                                keyword.lower() in post.selftext.lower()
                                for keyword in self.keywords
                            ):
                                continue

                            post_data = self._extract_post_data(post, subreddit_name)
                            data.append(post_data)

                            # Get top comments for this post
                            post.comments.replace_more(limit=0)
                            for comment in post.comments[:self.comment_limit]:
                                if comment.score >= self.min_score:
                                    comment_data = self._extract_comment_data(
                                        comment, subreddit_name, post.id
                                    )
                                    data.append(comment_data)

                except Exception as e:
                    logger.error(f'Error collecting from subreddit {subreddit_name}: {e}')
                    continue

            logger.debug(f'Collected {len(data)} items from Reddit')
            return data

        except Exception as e:
            logger.error(f'Error in Reddit data collection: {e}')
            return []

    def _extract_post_data(self, post: Any, subreddit: str) -> Dict[str, Any]:
        """
        Extract relevant data from a Reddit post.

        Args:
            post: PRAW post object
            subreddit: Subreddit name

        Returns:
            Extracted post data
        """
        return {
            'id': post.id,
            'type': 'post',
            'title': post.title,
            'content': post.selftext or '',
            'author': str(post.author) if post.author else '[deleted]',
            'subreddit': subreddit,
            'score': post.score,
            'upvote_ratio': post.upvote_ratio,
            'num_comments': post.num_comments,
            'created_utc': datetime.fromtimestamp(
                post.created_utc, tz=timezone.utc
            ).isoformat(),
            'url': post.url,
            'permalink': f"https://reddit.com{post.permalink}",
            'is_original_content': post.is_original_content,
            'link_flair_text': post.link_flair_text,
            'over_18': post.over_18,
            'awards': len(post.all_awardings) if hasattr(post, 'all_awardings') else 0,
            'distinguished': post.distinguished,
            'stickied': post.stickied,
            'comments': []  # Comments collected separately
        }

    def _extract_comment_data(
        self, comment: Any, subreddit: str, post_id: str
    ) -> Dict[str, Any]:
        """
        Extract relevant data from a Reddit comment.

        Args:
            comment: PRAW comment object
            subreddit: Subreddit name
            post_id: Parent post ID

        Returns:
            Extracted comment data
        """
        return {
            'id': comment.id,
            'type': 'comment',
            'title': '',  # Comments don't have titles
            'content': comment.body,
            'author': str(comment.author) if comment.author else '[deleted]',
            'subreddit': subreddit,
            'score': comment.score,
            'upvote_ratio': None,  # Comments don't have upvote ratio
            'num_comments': 0,  # Comments don't have comment count
            'created_utc': datetime.fromtimestamp(
                comment.created_utc, tz=timezone.utc
            ).isoformat(),
            'url': f"https://reddit.com{comment.permalink}",
            'permalink': f"https://reddit.com{comment.permalink}",
            'is_original_content': False,
            'link_flair_text': None,
            'over_18': False,
            'awards': 0,
            'distinguished': comment.distinguished,
            'stickied': comment.stickied,
            'post_id': post_id,
            'parent_id': comment.parent_id,
            'depth': comment.depth if hasattr(comment, 'depth') else 0
        }

    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform Reddit data into standardized format.

        Args:
            raw_data: Raw Reddit data

        Returns:
            Transformed data
        """
        transformed = {
            'platform': 'reddit',
            'platform_id': raw_data['id'],
            'item_type': raw_data['type'],
            'title': raw_data['title'],
            'text': raw_data['content'],
            'author': raw_data['author'],
            'community': raw_data['subreddit'],
            'score': raw_data['score'],
            'upvote_ratio': raw_data['upvote_ratio'],
            'comment_count': raw_data['num_comments'],
            'created_at': raw_data['created_utc'],
            'url': raw_data['url'],
            'permalink': raw_data['permalink'],
            'metadata': {
                'is_original_content': raw_data['is_original_content'],
                'link_flair_text': raw_data['link_flair_text'],
                'over_18': raw_data['over_18'],
                'awards': raw_data['awards'],
                'distinguished': raw_data['distinguished'],
                'stickied': raw_data['stickied']
            }
        }

        # Add comment-specific fields
        if raw_data['type'] == 'comment':
            transformed['metadata'].update({
                'post_id': raw_data.get('post_id'),
                'parent_id': raw_data.get('parent_id'),
                'depth': raw_data.get('depth', 0)
            })

        # Add language detection placeholder (will be filled in preprocessing)
        transformed['language'] = None
        transformed['text_clean'] = None

        return transformed

    def get_subreddit_stats(self, subreddit_name: str) -> Dict[str, Any]:
        """
        Get statistics for a specific subreddit.

        Args:
            subreddit_name: Name of the subreddit

        Returns:
            Subreddit statistics
        """
        try:
            subreddit = self.reddit.subreddit(subreddit_name)

            return {
                'name': subreddit.display_name,
                'subscribers': subreddit.subscribers,
                'active_users': subreddit.active_user_count,
                'created_utc': datetime.fromtimestamp(
                    subreddit.created_utc, tz=timezone.utc
                ).isoformat() if subreddit.created_utc else None,
                'description': subreddit.public_description,
                'over18': subreddit.over18
            }

        except Exception as e:
            logger.error(f'Error getting subreddit stats for {subreddit_name}: {e}')
            return {}