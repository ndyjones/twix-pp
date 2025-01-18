# tests/preprocessing/test_archive_processor.py
import pytest
from pathlib import Path
import json
import pandas as pd
from datetime import datetime
import pytz
import logging
from twitter_analysis.preprocessing.archive_processor import TwitterArchivePreprocessor, Tweet, TweetMedia

@pytest.fixture
def sample_tweets_js():
    """Create a sample tweets.js file content"""
    return [
        {
            "tweet": {
                "id_str": "123456789",
                "full_text": "Hello world! #test https://example.com",
                "created_at": "Wed Oct 10 20:19:24 +0000 2018",
                "favorite_count": 5,
                "retweet_count": 2,
                "entities": {
                    "hashtags": [{"text": "test"}],
                    "urls": [{"expanded_url": "https://example.com"}],
                    "media": []
                },
                "lang": "en"
            }
        },
        {
            "tweet": {
                "id_str": "987654321",
                "full_text": "RT @someone: Great tweet with media! 📸",
                "created_at": "Wed Oct 11 10:19:24 +0000 2018",
                "favorite_count": 0,
                "retweet_count": 0,
                "entities": {
                    "hashtags": [],
                    "urls": [],
                    "media": [{
                        "type": "photo",
                        "media_url": "http://pbs.twimg.com/media/sample.jpg"
                    }]
                },
                "retweeted_status": {},
                "lang": "en"
            }
        }
    ]

@pytest.fixture
def temp_archive(tmp_path, sample_tweets_js):
    """Create a temporary archive structure with sample data"""
    # Create directory structure
    archive_dir = tmp_path / "twitter_archive"
    data_dir = archive_dir / "data"
    assets_dir = archive_dir / "assets"
    
    data_dir.mkdir(parents=True)
    assets_dir.mkdir()
    
    # Write sample tweets file
    tweets_file = data_dir / "tweets.js"
    tweets_file.write_text(json.dumps(sample_tweets_js))
    
    return archive_dir

@pytest.fixture
def preprocessor(temp_archive, tmp_path):
    """Create a TwitterArchivePreprocessor instance with temporary directories"""
    output_dir = tmp_path / "output"
    return TwitterArchivePreprocessor(
        archive_path=str(temp_archive),
        output_path=str(output_dir),
        save_formats=['parquet', 'csv']
    )

def test_initialization(preprocessor):
    """Test that the preprocessor initializes correctly"""
    assert preprocessor.archive_path.exists()
    assert preprocessor.output_path.exists()
    assert preprocessor.data_path.exists()
    assert preprocessor.assets_path.exists()
    assert isinstance(preprocessor.logger, logging.Logger)

def test_load_json_file(preprocessor, temp_archive):
    """Test loading and parsing of JSON files"""
    json_file = preprocessor.data_path / "tweets.js"
    data = preprocessor.load_json_file(json_file)
    
    assert isinstance(data, list)
    assert len(data) == 2
    assert "tweet" in data[0]
    assert data[0]["tweet"]["id_str"] == "123456789"

def test_process_tweet(preprocessor):
    """Test processing of individual tweets"""
    sample_tweet_data = {
        "tweet": {
            "id_str": "123456789",
            "full_text": "Hello world! #test",
            "created_at": "Wed Oct 10 20:19:24 +0000 2018",
            "favorite_count": 5,
            "retweet_count": 2,
            "entities": {
                "hashtags": [{"text": "test"}],
                "urls": [],
                "media": []
            },
            "lang": "en"
        }
    }
    
    tweet = preprocessor.process_tweet(sample_tweet_data)
    
    assert isinstance(tweet, Tweet)
    assert tweet.id == "123456789"
    assert tweet.text == "Hello world! #test"
    assert isinstance(tweet.created_at, datetime)
    assert tweet.likes == 5
    assert tweet.retweets == 2
    assert tweet.hashtags == ["test"]
    assert not tweet.is_retweet

def test_process_archive(preprocessor):
    """Test processing of the entire archive"""
    df = preprocessor.process_archive()
    
    # Check DataFrame properties
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert all(col in df.columns for col in [
        'id', 'text', 'created_at', 'likes', 'retweets', 'hashtags',
        'has_media', 'tweet_length', 'hour_of_day', 'day_of_week'
    ])
    
    # Check data types
    assert pd.api.types.is_datetime64_any_dtype(df['created_at'])
    assert pd.api.types.is_integer_dtype(df['likes'])
    assert pd.api.types.is_integer_dtype(df['retweets'])

def test_save_formats(preprocessor):
    """Test that files are saved in specified formats"""
    df = preprocessor.process_archive()
    
    # Check that files were created
    assert (preprocessor.output_path / 'tweets_processed.parquet').exists()
    assert (preprocessor.output_path / 'tweets_processed.csv').exists()
    
    # Check that files can be read back
    df_parquet = pd.read_parquet(preprocessor.output_path / 'tweets_processed.parquet')
    df_csv = pd.read_csv(preprocessor.output_path / 'tweets_processed.csv')
    
    assert len(df_parquet) == len(df)
    assert len(df_csv) == len(df)

def test_error_handling(preprocessor, tmp_path):
    """Test error handling for invalid files and data"""
    # Test with invalid JSON file
    invalid_file = tmp_path / "invalid.js"
    invalid_file.write_text("invalid json content")
    
    result = preprocessor.load_json_file(invalid_file)
    assert result == {}  # Should return empty dict on error
    
    # Test with invalid tweet data
    invalid_tweet = {"tweet": {"id_str": "123"}}  # Missing required fields
    result = preprocessor.process_tweet(invalid_tweet)
    assert result is None  # Should return None on error

def test_media_handling(preprocessor):
    """Test handling of tweets with media attachments"""
    tweet_with_media = {
        "tweet": {
            "id_str": "123456789",
            "full_text": "Tweet with media",
            "created_at": "Wed Oct 10 20:19:24 +0000 2018",
            "entities": {
                "media": [{
                    "type": "photo",
                    "media_url": "http://example.com/image.jpg"
                }]
            },
            "lang": "en"
        }
    }
    
    tweet = preprocessor.process_tweet(tweet_with_media)
    assert len(tweet.media) == 1
    assert isinstance(tweet.media[0], TweetMedia)
    assert tweet.media[0].type == "photo"

def test_summary_stats(preprocessor):
    """Test generation of summary statistics"""
    df = preprocessor.process_archive()
    stats = preprocessor.generate_summary_stats(df)
    
    assert isinstance(stats, dict)
    assert 'total_tweets' in stats
    assert 'engagement' in stats
    assert 'content_analysis' in stats
    assert 'timing' in stats
    
    # Check specific stats
    assert stats['total_tweets'] == 2
    assert isinstance(stats['engagement']['total_likes'], (int, float))
    assert isinstance(stats['content_analysis']['avg_tweet_length'], float)