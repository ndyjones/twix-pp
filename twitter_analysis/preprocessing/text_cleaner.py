import re
from typing import Set, List
import unicodedata
import emoji

class TextCleaner:
    def __init__(self):
        self.url_pattern = re.compile(r'https?://\S+|www\.\S+')
        self.mention_pattern = re.compile(r'@\w+')
        self.hashtag_pattern = re.compile(r'#\w+')
        self.email_pattern = re.compile(r'\S+@\S+')
        self.number_pattern = re.compile(r'\d+')
        self.unicode_pattern = re.compile(r'[^\x00-\x7F]+')
        
    def clean_text(self, text: str, 
                  remove_urls: bool = True,
                  remove_mentions: bool = False,
                  remove_hashtags: bool = False,
                  remove_emails: bool = True,
                  remove_numbers: bool = False,
                  normalize_unicode: bool = True,
                  preserve_emojis: bool = True) -> str:
        """
        Clean text with configurable options
        """
        if not text:
            return ""

        # Store emojis if needed
        emoji_list = []
        if preserve_emojis:
            emoji_list = [c for c in text if c in emoji.EMOJI_DATA]
            
        # Basic cleaning
        text = text.strip()
        
        # Replace HTML entities
        text = text.replace('&amp;', '&')
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        text = text.replace('&quot;', '"')
        text = text.replace('&#39;', "'")
        
        # Apply removals based on parameters
        if remove_urls:
            text = self.url_pattern.sub(' ', text)
        if remove_mentions:
            text = self.mention_pattern.sub(' ', text)
        if remove_hashtags:
            text = self.hashtag_pattern.sub(' ', text)
        if remove_emails:
            text = self.email_pattern.sub(' ', text)
        if remove_numbers:
            text = self.number_pattern.sub(' ', text)
            
        # Unicode normalization
        if normalize_unicode:
            text = unicodedata.normalize('NFKC', text)
            
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        # Reinsert emojis if preserved
        if preserve_emojis and emoji_list:
            text = text + ' ' + ' '.join(emoji_list)
            
        return text

    def extract_entities(self, text: str) -> dict:
        """
        Extract various entities from text
        """
        return {
            'urls': self.url_pattern.findall(text),
            'mentions': self.mention_pattern.findall(text),
            'hashtags': self.hashtag_pattern.findall(text),
            'emojis': [c for c in text if c in emoji.EMOJI_DATA],
            'emails': self.email_pattern.findall(text)
        }

    def normalize_hashtags(self, hashtags: List[str]) -> List[str]:
        """
        Normalize hashtags (e.g., #MachineLearning -> machine learning)
        """
        def split_camel_case(tag: str) -> str:
            # Remove # if present
            tag = tag.lstrip('#')
            # Split camel case
            words = re.findall(r'[A-Z]?[a-z]+|[A-Z]{2,}(?=[A-Z][a-z]|\d|\W|$)|\d+', tag)
            return ' '.join(word.lower() for word in words)
            
        return [split_camel_case(tag) for tag in hashtags]