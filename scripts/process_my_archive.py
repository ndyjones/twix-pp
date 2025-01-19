"""Personal script for processing my Twitter archive."""
from pathlib import Path
import logging
from datetime import datetime
import time
import sys
from twitter_analysis.preprocessing import TwitterArchivePreprocessor

# Full paths to external data directory
ARCHIVE_PATH = "/mnt/g/Dropbox/[Documents]/dev/twix-data/twitter_archive"
OUTPUT_PATH = "/mnt/g/Dropbox/[Documents]/dev/twix-data/processed"
LOG_PATH = "/mnt/g/Dropbox/[Documents]/dev/twix-data/logs"

def setup_logging():
    """Set up logging with both file and console output."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = Path(LOG_PATH) / f"processing_{timestamp}.log"
    
    # Create logs directory if it doesn't exist
    Path(LOG_PATH).mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return log_file

def main():
    log_file = setup_logging()
    logger = logging.getLogger(__name__)
    
    start_time = time.time()
    logger.info(f"Starting Twitter archive processing")
    logger.info(f"Archive path: {ARCHIVE_PATH}")
    logger.info(f"Output path: {OUTPUT_PATH}")
    logger.info(f"Log file: {log_file}")
    
    try:
        # Initialize processor
        processor = TwitterArchivePreprocessor(
            archive_path=ARCHIVE_PATH,
            output_path=OUTPUT_PATH,
            save_formats=['csv', 'parquet', 'json']
        )
        
        # Process archive
        logger.info("Beginning archive processing...")
        df = processor.process_archive()
        
        # Generate stats
        logger.info("Generating summary statistics...")
        stats = processor.generate_summary_stats(df)
        
        # Log summary information
        logger.info("\nProcessing Summary:")
        logger.info(f"Total tweets processed: {stats['total_tweets']}")
        logger.info(f"Date range: {stats['date_range']['start']} to {stats['date_range']['end']}")
        logger.info(f"Total likes: {stats['engagement']['total_likes']}")
        logger.info(f"Total retweets: {stats['engagement']['total_retweets']}")
        
        # Calculate and log processing time
        end_time = time.time()
        processing_time = end_time - start_time
        logger.info(f"\nProcessing completed in {processing_time:.2f} seconds")
        logger.info(f"Output files saved to: {OUTPUT_PATH}")
        
    except KeyboardInterrupt:
        logger.warning("\nProcessing interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An error occurred during processing: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()