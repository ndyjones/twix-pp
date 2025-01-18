# tests/preprocessing/test_text_cleaner.py
import pytest
from twitter_analysis.preprocessing.text_cleaner import TextCleaner

def test_basic_text_cleaning():
    cleaner = TextCleaner()
    text = "Hello &amp; world! https://example.com @username #hashtag"
    
    # Test with default settings
    cleaned = cleaner.clean_text(text)
    assert "Hello & world!" in cleaned
    assert "https://example.com" not in cleaned  # URLs removed by default
    
def test_emoji_preservation():
    cleaner = TextCleaner()
    text = "Hello 👋 World! 🌍"
    
    cleaned = cleaner.clean_text(text, preserve_emojis=True)
    assert "👋" in cleaned
    assert "🌍" in cleaned

def test_hashtag_handling():
    cleaner = TextCleaner()
    text = "Check out #MachineLearning and #AI"
    
    # Test with hashtags preserved
    cleaned = cleaner.clean_text(text, remove_hashtags=False)
    assert "#MachineLearning" in cleaned
    
    # Test with hashtags removed
    cleaned = cleaner.clean_text(text, remove_hashtags=True)
    assert "#MachineLearning" not in cleaned