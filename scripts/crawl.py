"""
Main script for PARALLEL multi-category crawling
Fast crawling with concurrent workers
"""

import json
import logging
import argparse
from datetime import datetime
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from crawler.crawler_parallel import TikiParallelCrawler


def setup_logging(config: dict):
    """Setup logging"""
    log_config = config.get('logging', {})
    log_dir = Path(log_config.get('log_dir', 'logs/crawler'))
    log_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'parallel_crawl_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)


def load_config(config_path: str = 'config/config.json') -> dict:
    """Load configuration"""
    with open(config_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_crawl_data(data: dict, output_dir: Path):
    """Save crawl results to JSON"""
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = output_dir / f'parallel_crawl_results_{timestamp}.json'
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    return filename




def run_parallel_crawl(config: dict, logger):
    """Run parallel multi-category crawl (only crawl, no database storage)"""
    logger.info("=" * 70)
    logger.info("STARTING PARALLEL MULTI-CATEGORY CRAWL")
    logger.info("=" * 70)
    
    start_time = datetime.now()
    
    # Initialize crawler
    crawler = TikiParallelCrawler(config)
    
    try:
        # Crawl all categories with parallel workers
        results = crawler.crawl_all_categories_parallel()
        
        # Save raw results to JSON
        data_dir = Path(config.get('database', {}).get('data_dir', 'data/raw'))
        json_file = save_crawl_data(results, data_dir)
        logger.info(f"Raw results saved to: {json_file}")
        
        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info("=" * 70)
        logger.info("PARALLEL CRAWL COMPLETED")
        logger.info(f"Duration: {duration/60:.1f} minutes ({duration:.1f} seconds)")
        logger.info(f"Categories: {results['stats']['total_categories']}")
        logger.info(f"Products crawled: {results['stats']['successful_products']}/{results['stats']['total_products']}")
        logger.info(f"Overall speed: {results.get('overall_speed', 0):.2f} products/second")
        logger.info(f"JSON file: {json_file}")
        logger.info("")
        logger.info("Note: Use build_db.py or update_db.py to import data into database")
        logger.info("=" * 70)
        
    except KeyboardInterrupt:
        logger.warning("Crawl interrupted by user")
    except Exception as e:
        logger.error(f"Crawl failed: {e}", exc_info=True)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Parallel multi-category Tiki crawler')
    parser.add_argument('--config', default='config/config.json', help='Config file path')
    parser.add_argument('--workers', type=int, help='Number of parallel workers (overrides config)')
    parser.add_argument('--rate-limit', type=int, help='Max requests per second (overrides config)')
    
    args = parser.parse_args()
    
    # Load config and setup
    config = load_config(args.config)
    
    # Override config with command line args
    if args.workers:
        config['crawler']['max_workers'] = args.workers
    if args.rate_limit:
        config['crawler']['rate_limit_per_second'] = args.rate_limit
    
    logger = setup_logging(config)
    
    logger.info(f"Config: {args.config}")
    logger.info(f"Workers: {config.get('crawler', {}).get('max_workers', 10)}")
    logger.info(f"Rate limit: {config.get('crawler', {}).get('rate_limit_per_second', 5)} req/s")
    
    run_parallel_crawl(config, logger)


if __name__ == '__main__':
    main()
