import json
import os
from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
import logging
from pathlib import Path
import re
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

@dataclass
class TweetMedia:
    type: str
    url: str
    local_path: Optional[str] = None

@dataclass
class Tweet:
    id: str
    text: str
    created_at: datetime
    likes: int
    retweets: int
    hashtags: List[str]
    urls: List[str]
    media: List[TweetMedia]
    is_retweet: bool
    conversation_id: str
    in_reply_to_user_id: Optional[str]
    lang: str

class TwitterArchivePreprocessor:
    def __init__(self, archive_path: str, output_path: str):
        """Initialize the preprocessor with paths and setup logging."""
        self.archive_path = Path(archive_path)
        self.output_path = Path(output_path)
        self.data_path = self.archive_path / "data"
        self.assets_path = self.archive_path / "assets"
        self.setup_logging()
        
    def setup_logging(self):
        """Configure logging with timestamps and levels."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.output_path / 'preprocessing.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def load_json_file(self, file_path: Path) -> Dict:
        """Load and parse a JSON file, handling Twitter's JS format."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                # Remove potential JavaScript wrapper
                if content.startswith('window.YTD.'):
                    content = content[content.index('['):]
                return json.loads(content)
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON from {file_path}: {e}")
            return {}
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {e}")
            return {}

    def parse_datetime(self, date_str: str) -> datetime:
        """Parse Twitter's datetime format."""
        return datetime.strptime(date_str, '%a %b %d %H:%M:%S %z %Y')

    def clean_text(self, text: str) -> str:
        """Clean tweet text by handling HTML entities and normalizing whitespace."""
        # Handle common HTML entities
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        # Normalize whitespace
        text = ' '.join(text.split())
        return text

    def process_tweet(self, tweet_data: Dict) -> Optional[Tweet]:
        """Process a single tweet into a structured Tweet object."""
        try:
            tweet = tweet_data.get('tweet', tweet_data)  # Handle nested structure
            
            # Extract media information
            media_list = []
            for media in tweet.get('entities', {}).get('media', []):
                media_list.append(TweetMedia(
                    type=media.get('type', 'unknown'),
                    url=media.get('media_url', ''),
                    local_path=self.find_local_media(media.get('media_url', ''))
                ))

            return Tweet(
                id=tweet.get('id_str', ''),
                text=self.clean_text(tweet.get('full_text', tweet.get('text', ''))),
                created_at=self.parse_datetime(tweet.get('created_at', '')),
                likes=tweet.get('favorite_count', 0),
                retweets=tweet.get('retweet_count', 0),
                hashtags=[h['text'] for h in tweet.get('entities', {}).get('hashtags', [])],
                urls=[u['expanded_url'] for u in tweet.get('entities', {}).get('urls', [])],
                media=media_list,
                is_retweet='retweeted_status' in tweet,
                conversation_id=tweet.get('conversation_id_str', ''),
                in_reply_to_user_id=tweet.get('in_reply_to_user_id_str'),
                lang=tweet.get('lang', 'unknown')
            )
        except Exception as e:
            self.logger.error(f"Error processing tweet {tweet_data.get('id_str', 'unknown')}: {e}")
            return None

    def find_local_media(self, media_url: str) -> Optional[str]:
        """Find corresponding local media file in assets directory."""
        if not media_url:
            return None
            
        # Extract filename from URL
        filename = media_url.split('/')[-1]
        
        # Check different possible locations
        possible_paths = [
            self.assets_path / filename,
            self.assets_path / "media" / filename,
            self.assets_path / "images" / filename
        ]
        
        for path in possible_paths:
            if path.exists():
                return str(path)
        return None

    def process_archive(self) -> pd.DataFrame:
        """Process the entire Twitter archive."""
        self.logger.info("Starting Twitter archive processing")
        
        tweets_data = []
        
        # Process all JSON files in parallel
        json_files = list(self.data_path.glob('*.js'))
        
        with ThreadPoolExecutor() as executor:
            for file_path in tqdm(json_files, desc="Processing files"):
                tweet_list = self.load_json_file(file_path)
                if tweet_list:
                    futures = [executor.submit(self.process_tweet, tweet) for tweet in tweet_list]
                    for future in futures:
                        tweet = future.result()
                        if tweet:
                            tweets_data.append(tweet)
        
        # Convert to DataFrame
        df = pd.DataFrame([vars(tweet) for tweet in tweets_data])
        
        # Add derived features
        df['has_media'] = df['media'].apply(lambda x: len(x) > 0)
        df['tweet_length'] = df['text'].str.len()
        df['hour_of_day'] = df['created_at'].dt.hour
        df['day_of_week'] = df['created_at'].dt.day_name()
        
        # Sort by timestamp
        df.sort_values('created_at', inplace=True)
        
        # Save to both CSV and parquet formats
        df.to_csv(self.output_path / 'tweets_processed.csv', index=False)
        df.to_parquet(self.output_path / 'tweets_processed.parquet')
        
        self.logger.info(f"Processing complete. Processed {len(df)} tweets.")
        return df

    def generate_summary_stats(self, df: pd.DataFrame) -> Dict:
        """Generate summary statistics for the processed tweets."""
        stats = {
            'total_tweets': len(df),
            'date_range': {
                'start': df['created_at'].min(),
                'end': df['created_at'].max()
            },
            'engagement': {
                'total_likes': df['likes'].sum(),
                'total_retweets': df['retweets'].sum(),
                'avg_likes_per_tweet': df['likes'].mean(),
                'avg_retweets_per_tweet': df['retweets'].mean()
            },
            'content_analysis': {
                'tweets_with_media': df['has_media'].sum(),
                'tweets_with_urls': df['urls'].apply(len).sum(),
                'most_common_hashtags': df['hashtags'].explode().value_counts().head(10).to_dict(),
                'avg_tweet_length': df['tweet_length'].mean()
            },
            'timing': {
                'most_active_hours': df['hour_of_day'].value_counts().head(5).to_dict(),
                'most_active_days': df['day_of_week'].value_counts().to_dict()
            }
        }
        
        # Save summary stats
        with open(self.output_path / 'summary_stats.json', 'w') as f:
            json.dump(stats, f, default=str, indent=2)
            
        return stats
