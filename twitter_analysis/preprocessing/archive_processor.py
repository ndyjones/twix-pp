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

    def __init__(self, archive_path: str, output_path: str, save_formats: List[str] = None):
        """Initialize the preprocessor with paths and setup logging.
        
        Args:
            archive_path (str): Path to the Twitter archive directory
            output_path (str): Path where processed files will be saved
            save_formats (List[str], optional): List of formats to save data in. 
            Supported formats: 'parquet', 'csv', 'json'. Defaults to ['parquet', 'csv'].
        """
        self.archive_path = Path(archive_path)
        self.output_path = Path(output_path)
        self.data_path = self.archive_path / "data"
        self.assets_path = self.archive_path / "assets"
        self.save_formats = save_formats or ['parquet', 'csv']
        self.output_path.mkdir(parents=True, exist_ok=True) # Create output directory if it doesn't exist
        self.setup_logging()   

    def load_json_file(self, file_path: Path) -> Dict:
        """Load and parse a JSON file, handling Twitter's JS format."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                # Remove JavaScript wrapper if present
                if content.startswith('window.YTD.'):
                    content = content[content.index('['):]
                # Parse JSON
                data = json.loads(content)
                self.logger.debug(f"Successfully loaded {file_path}")
                return data
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
            tweet = tweet_data.get('tweet', tweet_data)
            tweet_id = tweet.get('id_str', 'unknown')
            
            # Handle date parsing with better error reporting
            created_at = tweet.get('created_at', '')
            try:
                parsed_date = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y') if created_at else datetime.now(pytz.UTC)
            except ValueError:
                self.logger.debug(f"Tweet {tweet_id}: Invalid date format '{created_at}', using current time")
                parsed_date = datetime.now(pytz.UTC)
                
            # Extract media information
            media_list = []
            entities = tweet.get('entities', {})
            extended_entities = tweet.get('extended_entities', {})
            
            # Combine media from both sources
            media_items = entities.get('media', []) + extended_entities.get('media', [])
            for media in media_items:
                media_type = media.get('type', 'unknown')
                media_url = media.get('media_url', '') or media.get('media_url_https', '')
                
                media_list.append(TweetMedia(
                    type=media_type,
                    url=media_url,
                    local_path=self.find_local_media(media_url)
                ))

            # Ensure numeric values
            likes = int(tweet.get('favorite_count', 0) or 0)
            retweets = int(tweet.get('retweet_count', 0) or 0)

            return Tweet(
                id=tweet_id,
                text=self.clean_text(tweet.get('full_text', tweet.get('text', ''))),
                created_at=parsed_date,
                likes=likes,
                retweets=retweets,
                hashtags=[h.get('text', '') for h in entities.get('hashtags', [])],
                urls=[u.get('expanded_url', '') for u in entities.get('urls', [])],
                media=media_list,
                is_retweet=bool(tweet.get('retweeted_status')),
                conversation_id=tweet.get('conversation_id_str', ''),
                in_reply_to_user_id=tweet.get('in_reply_to_user_id_str'),
                lang=tweet.get('lang', 'unknown')
            )
        except Exception as e:
            self.logger.error(f"Error processing tweet {tweet_id}: {e}")
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
        self.logger.info(f"Found {len(json_files)} JSON files")
        
        # Debug: Print the first tweet data
        if json_files:
            first_file_content = self.load_json_file(json_files[0])
            self.logger.info(f"First file content structure: {type(first_file_content)}")
            if first_file_content:
                self.logger.info(f"First tweet structure: {first_file_content[0] if isinstance(first_file_content, list) else first_file_content}")
        
        with ThreadPoolExecutor() as executor:
            for file_path in tqdm(json_files, desc="Processing files"):
                tweet_list = self.load_json_file(file_path)
                if tweet_list:
                    futures = [executor.submit(self.process_tweet, tweet) for tweet in tweet_list]
                    for future in futures:
                        tweet = future.result()
                        if tweet:
                            tweets_data.append(tweet)
        
        # Debug: Print structure of processed tweets
        if tweets_data:
            self.logger.info(f"First processed tweet structure: {vars(tweets_data[0])}")
        
        # Convert to DataFrame
        df = pd.DataFrame([vars(tweet) for tweet in tweets_data])
        
        # Debug: Print DataFrame columns
        self.logger.info(f"DataFrame columns: {df.columns.tolist()}")
        
        # Convert media objects to serializable format
        if 'media' in df.columns:
            df['media'] = df['media'].apply(lambda x: [{'type': m.type, 'url': m.url, 'local_path': m.local_path} for m in x])
            self.logger.info("Successfully converted media objects to serializable format")
        else:
            self.logger.warning("No 'media' column found in DataFrame")
        
        try:
            # Add derived features
            df['has_media'] = df['media'].apply(lambda x: len(x) > 0) if 'media' in df.columns else False
            df['tweet_length'] = df['text'].str.len()
            df['hour_of_day'] = df['created_at'].dt.hour
            df['day_of_week'] = df['created_at'].dt.day_name()
            
            # Sort by timestamp
            df.sort_values('created_at', inplace=True)
            
            # Save in specified formats
            for format_type in self.save_formats:
                try:
                    if format_type.lower() == 'csv':
                        df.to_csv(self.output_path / 'tweets_processed.csv', index=False)
                        self.logger.info(f"Saved CSV file to {self.output_path / 'tweets_processed.csv'}")
                    elif format_type.lower() == 'parquet':
                        # Create a copy of the DataFrame with serializable types
                        df_parquet = df.copy()
                        df_parquet['media'] = df_parquet['media'].apply(json.dumps) if 'media' in df_parquet.columns else '[]'
                        df_parquet.to_parquet(self.output_path / 'tweets_processed.parquet')
                        self.logger.info(f"Saved Parquet file to {self.output_path / 'tweets_processed.parquet'}")
                    elif format_type.lower() == 'json':
                        df.to_json(self.output_path / 'tweets_processed.json', orient='records', date_format='iso')
                        self.logger.info(f"Saved JSON file to {self.output_path / 'tweets_processed.json'}")
                    else:
                        self.logger.warning(f"Unsupported format type: {format_type}")
                except Exception as e:
                    self.logger.error(f"Error saving to {format_type} format: {e}")
        except Exception as e:
            self.logger.error(f"Error during DataFrame processing: {str(e)}")
            raise
        
        self.logger.info(f"Processing complete. Processed {len(df)} tweets.")
        return df

    def generate_summary_stats(self, df: pd.DataFrame) -> Dict:
        """Generate summary statistics for the processed tweets."""
        stats = {
            'total_tweets': int(len(df)),
            'date_range': {
                'start': df['created_at'].min(),
                'end': df['created_at'].max()
            },
            'engagement': {
                'total_likes': int(df['likes'].sum()),
                'total_retweets': int(df['retweets'].sum()),
                'avg_likes_per_tweet': float(df['likes'].mean()),
                'avg_retweets_per_tweet': float(df['retweets'].mean())
            },
            'content_analysis': {
                'tweets_with_media': int(df['has_media'].sum()),
                'tweets_with_urls': int(df['urls'].apply(len).sum()),
                'most_common_hashtags': dict(df['hashtags'].explode().value_counts().head(10)),
                'avg_tweet_length': float(df['tweet_length'].mean())
            },
            'timing': {
                'most_active_hours': dict(df['hour_of_day'].value_counts().head(5)),
                'most_active_days': dict(df['day_of_week'].value_counts())
            }
        }
        
        # Save summary stats
        with open(self.output_path / 'summary_stats.json', 'w') as f:
            json.dump(stats, f, default=str, indent=2)
                
        return stats