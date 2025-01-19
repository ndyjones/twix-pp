import os
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import shutil
import hashlib
import logging
import mimetypes
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import json

@dataclass
class MediaFile:
    """Represents a media file from the Twitter archive"""
    file_id: str
    original_path: Path
    media_type: str
    tweet_ids: Set[str]
    size_bytes: int
    hash_md5: str
    width: Optional[int] = None
    height: Optional[int] = None

class MediaHandler:
    """Handles processing and organization of media files from Twitter archive"""
    
    def __init__(self, archive_path: str, output_path: str):
        """
        Initialize the MediaHandler.
        
        Args:
            archive_path: Path to the Twitter archive
            output_path: Path where processed media will be stored
        """
        self.archive_path = Path(archive_path)
        self.output_path = Path(output_path)
        self.media_path = self.archive_path / "data" / "media"
        self.assets_path = self.archive_path / "assets"
        self.logger = logging.getLogger(__name__)
        
        # Ensure output directories exist
        self.processed_media_path = self.output_path / "processed_media"
        self.processed_media_path.mkdir(parents=True, exist_ok=True)
        
        # Track processed files
        self.media_inventory: Dict[str, MediaFile] = {}

    def calculate_file_hash(self, file_path: Path) -> str:
        """Calculate MD5 hash of a file."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def detect_media_type(self, file_path: Path) -> str:
        """Detect the media type of a file."""
        mime_type, _ = mimetypes.guess_type(str(file_path))
        if mime_type:
            return mime_type
        # Fallback to basic extension check
        return f"application/{file_path.suffix[1:]}" if file_path.suffix else "application/octet-stream"

    def process_media_file(self, file_path: Path, tweet_id: str) -> Optional[MediaFile]:
        """Process a single media file."""
        try:
            if not file_path.exists():
                self.logger.warning(f"Media file not found: {file_path}")
                return None

            file_id = file_path.stem
            file_hash = self.calculate_file_hash(file_path)
            
            # Check if we've already processed this file
            if file_id in self.media_inventory:
                self.media_inventory[file_id].tweet_ids.add(tweet_id)
                return self.media_inventory[file_id]

            media_file = MediaFile(
                file_id=file_id,
                original_path=file_path,
                media_type=self.detect_media_type(file_path),
                tweet_ids={tweet_id},
                size_bytes=file_path.stat().st_size,
                hash_md5=file_hash
            )
            
            self.media_inventory[file_id] = media_file
            return media_file

        except Exception as e:
            self.logger.error(f"Error processing media file {file_path}: {e}")
            return None

    def organize_media(self) -> Dict[str, List[str]]:
        """
        Organize media files into a structured format.
        
        Returns:
            Dict mapping tweet IDs to lists of media file IDs
        """
        tweet_media_map: Dict[str, List[str]] = {}
        
        # Process media files in parallel
        with ThreadPoolExecutor() as executor:
            media_files = list(self.media_path.glob("*.*"))
            futures = []
            
            for file_path in media_files:
                # Extract tweet ID from media filename (assuming standard Twitter format)
                tweet_id = file_path.stem.split("-")[0]
                futures.append(executor.submit(self.process_media_file, file_path, tweet_id))
            
            for future in tqdm(futures, desc="Processing media files"):
                media_file = future.result()
                if media_file:
                    for tweet_id in media_file.tweet_ids:
                        if tweet_id not in tweet_media_map:
                            tweet_media_map[tweet_id] = []
                        tweet_media_map[tweet_id].append(media_file.file_id)

        return tweet_media_map

    def copy_to_processed(self, preserve_structure: bool = True) -> None:
        """
        Copy media files to processed directory with optional restructuring.
        
        Args:
            preserve_structure: If True, maintains original directory structure
        """
        for file_id, media_file in tqdm(self.media_inventory.items(), desc="Copying media files"):
            try:
                source_path = media_file.original_path
                if preserve_structure:
                    # Organize by media type
                    media_type_folder = media_file.media_type.split('/')[0]
                    dest_path = self.processed_media_path / media_type_folder / source_path.name
                else:
                    dest_path = self.processed_media_path / source_path.name
                
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_path, dest_path)
                
            except Exception as e:
                self.logger.error(f"Error copying media file {source_path}: {e}")

    def generate_media_report(self) -> None:
        """Generate a report of processed media files."""
        report = {
            "total_files": len(self.media_inventory),
            "total_size_bytes": sum(m.size_bytes for m in self.media_inventory.values()),
            "media_types": {},
            "duplicate_files": []
        }
        
        # Count media types
        for media_file in self.media_inventory.values():
            media_type = media_file.media_type
            if media_type not in report["media_types"]:
                report["media_types"][media_type] = 0
            report["media_types"][media_type] += 1
        
        # Find duplicates by hash
        hash_map = {}
        for media_file in self.media_inventory.values():
            if media_file.hash_md5 in hash_map:
                report["duplicate_files"].append({
                    "original": hash_map[media_file.hash_md5],
                    "duplicate": media_file.file_id
                })
            else:
                hash_map[media_file.hash_md5] = media_file.file_id
        
        # Save report
        report_path = self.output_path / "media_report.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        self.logger.info(f"Media report generated at {report_path}")