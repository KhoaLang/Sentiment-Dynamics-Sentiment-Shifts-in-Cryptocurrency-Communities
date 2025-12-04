"""
Text processing utilities for sentiment analysis.
"""

import re
import html
import pandas as pd
import langdetect
from typing import List, Dict, Any
import unicodedata
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.stem import WordNetLemmatizer

# Download required NLTK data (only once)
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

try:
    nltk.data.find('corpora/wordnet')
except LookupError:
    nltk.download('wordnet')

try:
    nltk.data.find('taggers/averaged_perceptron_tagger')
except LookupError:
    nltk.download('averaged_perceptron_tagger')


# Initialize NLTK components
STOP_WORDS = set(stopwords.words('english'))
LEMMATIZER = WordNetLemmatizer()

# Cryptocurrency-specific terms to preserve
CRYPTO_TERMS = {
    'bitcoin', 'btc', 'ethereum', 'eth', 'blockchain', 'crypto', 'defi',
    'nft', 'altcoin', 'hodl', 'satoshi', 'wallet', 'mining', 'staking',
    'dapp', 'dao', 'yield', 'liquidity', 'airdrop', 'whale', 'bullish',
    'bearish', 'fud', 'fomo', 'dyor', 'shill', 'pump', 'dump', 'ath',
    'atl', 'rekt', 'moon', 'lambo', 'wagmi', 'ngmi', 'gm', 'gn'
}

# Patterns for text cleaning
URL_PATTERN = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
MENTION_PATTERN = re.compile(r'@\w+')
HASHTAG_PATTERN = re.compile(r'#\w+')
EMOJI_PATTERN = re.compile(
    "["
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F680-\U0001F6FF"  # transport & map symbols
    "\U0001F1E0-\U0001F1FF"  # flags (iOS)
    "\U00002702-\U000027B0"
    "\U000024C2-\U0001F251"
    "]+",
    flags=re.UNICODE
)
WHITESPACE_PATTERN = re.compile(r'\s+')
SPECIAL_CHAR_PATTERN = re.compile(r'[^\w\s]')
REPEATED_CHAR_PATTERN = re.compile(r'(.)\1{2,}')  # Repeated characters (3 or more)


def clean_text(text: str,
              preserve_mentions: bool = False,
              preserve_hashtags: bool = False,
              preserve_urls: bool = False,
              preserve_emojis: bool = False) -> str:
    """
    Clean and normalize text content.

    Args:
        text: Input text to clean
        preserve_mentions: Whether to keep @mentions
        preserve_hashtags: Whether to keep #hashtags
        preserve_urls: Whether to keep URLs
        preserve_emojis: Whether to keep emojis

    Returns:
        Cleaned text
    """
    if not text or pd.isna(text):
        return ""

    # Convert to string and normalize unicode
    text = str(text)
    text = unicodedata.normalize('NFKC', text)

    # Remove HTML entities
    text = html.unescape(text)

    # Handle URLs
    if not preserve_urls:
        text = URL_PATTERN.sub(' ', text)

    # Handle mentions
    if not preserve_mentions:
        text = MENTION_PATTERN.sub(' ', text)
    else:
        # Replace with just the username without @
        text = MENTION_PATTERN.sub(lambda m: ' ' + m.group(0)[1:] + ' ', text)

    # Handle hashtags
    if not preserve_hashtags:
        text = HASHTAG_PATTERN.sub(' ', text)
    else:
        # Replace with just the tag without #
        text = HASHTAG_PATTERN.sub(lambda m: ' ' + m.group(0)[1:] + ' ', text)

    # Handle emojis
    if not preserve_emojis:
        text = EMOJI_PATTERN.sub(' ', text)

    # Convert to lowercase
    text = text.lower()

    # Remove repeated characters (e.g., "sooo" -> "so")
    text = REPEATED_CHAR_PATTERN.sub(lambda m: m.group(1) * 2, text)

    # Remove excessive whitespace
    text = WHITESPACE_PATTERN.sub(' ', text)

    # Remove special characters but preserve spaces
    text = SPECIAL_CHAR_PATTERN.sub(' ', text)

    # Remove extra spaces and strip
    text = WHITESPACE_PATTERN.sub(' ', text).strip()

    # Preserve cryptocurrency terms during stop word removal
    custom_stop_words = STOP_WORDS - CRYPTO_TERMS

    # Tokenize and remove stop words
    words = word_tokenize(text)
    filtered_words = [word for word in words if word not in custom_stop_words and len(word) > 1]

    # Lemmatize
    lemmatized_words = [LEMMATIZER.lemmatize(word) for word in filtered_words]

    # Rejoin
    cleaned_text = ' '.join(lemmatized_words)

    return cleaned_text


def detect_language_pandas(text: str) -> str:
    """
    Detect language using langdetect (pandas compatible).

    Args:
        text: Input text

    Returns:
        Language code (e.g., 'en', 'es', 'unknown')
    """
    try:
        if not text or pd.isna(text) or len(text.strip()) < 10:
            return "unknown"

        # Detect language
        lang = langdetect.detect(text.strip())
        return lang

    except:
        return "unknown"


def extract_hashtags(text: str) -> List[str]:
    """
    Extract hashtags from text.

    Args:
        text: Input text

    Returns:
        List of hashtags (without #)
    """
    hashtags = HASHTAG_PATTERN.findall(text)
    return [tag[1:] for tag in hashtags]  # Remove #


def extract_mentions(text: str) -> List[str]:
    """
    Extract mentions from text.

    Args:
        text: Input text

    Returns:
        List of mentions (without @)
    """
    mentions = MENTION_PATTERN.findall(text)
    return [mention[1:] for mention in mentions]  # Remove @


def extract_urls(text: str) -> List[str]:
    """
    Extract URLs from text.

    Args:
        text: Input text

    Returns:
        List of URLs
    """
    urls = URL_PATTERN.findall(text)
    return urls


def count_emojis(text: str) -> int:
    """
    Count emojis in text.

    Args:
        text: Input text

    Returns:
        Number of emojis
    """
    emojis = EMOJI_PATTERN.findall(text)
    return len(emojis)


def extract_cryptocurrencies(text: str) -> List[str]:
    """
    Extract cryptocurrency mentions from text.

    Args:
        text: Input text

    Returns:
        List of mentioned cryptocurrencies
    """
    # Convert to lowercase for matching
    text_lower = text.lower()

    found_cryptos = []

    # Check for known cryptocurrency terms
    crypto_keywords = {
        'bitcoin': 'bitcoin',
        'btc': 'bitcoin',
        'ethereum': 'ethereum',
        'eth': 'ethereum',
        'cardano': 'cardano',
        'ada': 'cardano',
        'solana': 'solana',
        'sol': 'solana',
        'dogecoin': 'dogecoin',
        'doge': 'dogecoin',
        'polkadot': 'polkadot',
        'dot': 'polkadot',
        'avalanche': 'avalanche',
        'avax': 'avalanche',
        'chainlink': 'chainlink',
        'link': 'chainlink',
        'polygon': 'polygon',
        'matic': 'polygon',
        'binance': 'binance',
        'bnb': 'binance',
        'ripple': 'ripple',
        'xrp': 'ripple',
        'litecoin': 'litecoin',
        'ltc': 'litecoin'
    }

    for keyword, crypto in crypto_keywords.items():
        if keyword in text_lower:
            found_cryptos.append(crypto)

    return list(set(found_cryptos))  # Remove duplicates


def calculate_readability_score(text: str) -> float:
    """
    Calculate a simple readability score.

    Args:
        text: Input text

    Returns:
        Readability score (0-1, higher is more readable)
    """
    if not text:
        return 0.0

    try:
        sentences = sent_tokenize(text)
        words = word_tokenize(text)

        if not sentences or not words:
            return 0.0

        # Simple metrics
        avg_sentence_length = len(words) / len(sentences)
        avg_word_length = sum(len(word) for word in words) / len(words)

        # Normalize scores (simple heuristic)
        length_score = min(1.0, 15 / max(1, avg_sentence_length))  # Prefer shorter sentences
        word_score = min(1.0, 5 / max(1, avg_word_length))  # Prefer shorter words

        readability = (length_score + word_score) / 2
        return readability

    except:
        return 0.5  # Default middle score


def extract_entities(text: str) -> Dict[str, List[str]]:
    """
    Extract various entities from text.

    Args:
        text: Input text

    Returns:
        Dictionary containing extracted entities
    """
    return {
        'hashtags': extract_hashtags(text),
        'mentions': extract_mentions(text),
        'urls': extract_urls(text),
        'cryptocurrencies': extract_cryptocurrencies(text),
        'emoji_count': count_emojis(text)
    }


def preprocess_text_batch(texts: pd.Series, **kwargs) -> pd.Series:
    """
    Preprocess a batch of texts using pandas.

    Args:
        texts: Pandas Series of texts
        **kwargs: Arguments for clean_text function

    Returns:
        Pandas Series of cleaned texts
    """
    return texts.apply(lambda x: clean_text(str(x), **kwargs) if pd.notna(x) else "")


def detect_language_batch(texts: pd.Series) -> pd.Series:
    """
    Detect languages for a batch of texts.

    Args:
        texts: Pandas Series of texts

    Returns:
        Pandas Series of language codes
    """
    return texts.apply(detect_language_pandas)


def extract_entities_batch(texts: pd.Series) -> pd.DataFrame:
    """
    Extract entities from a batch of texts.

    Args:
        texts: Pandas Series of texts

    Returns:
        DataFrame with extracted entities
    """
    entities_df = pd.DataFrame(texts.apply(extract_entities).tolist())
    entities_df.index = texts.index
    return entities_df