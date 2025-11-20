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
from crawler.database_v2 import DatabaseV2


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


def process_and_store_products(results: dict, db: DatabaseV2, logger):
    """Process crawl results and store in database"""
    logger.info("Storing products in database...")
    
    all_products = results.get('all_products', [])
    successful = 0
    failed = 0
    sellers_stored = set()  # Track unique sellers
    
    # Get start_time from crawl results to use as crawl_timestamp
    crawl_timestamp = results.get('start_time')
    
    for product in all_products:
        if not product.get('success'):
            failed += 1
            continue
        
        try:
            details = product.get('details', {})
            product_id = details.get('id')
            category_id = product.get('category_id')
            category_name = product.get('category_name')
            
            # Insert seller info first (if not already stored)
            seller_info = details.get('seller_info_enriched', {})
            seller_id = seller_info.get('id')
            if seller_id and seller_id not in sellers_stored:
                db.insert_seller({
                    'seller_id': seller_id,
                    'seller_name': seller_info.get('name'),
                    'seller_url': seller_info.get('link'),
                    'seller_total_follower': seller_info.get('total_follower')
                })
                sellers_stored.add(seller_id)
            
            # Insert product
            db.insert_product(details, category_id, category_name)
            
            # Insert price history with crawl_timestamp
            db.insert_price_history(product_id, details, crawl_timestamp)
            
            # Insert sales history with crawl_timestamp
            db.insert_sales_history(product_id, {
                'quantity_sold': details.get('quantity_sold', {}).get('value', 0),
                'all_time_quantity_sold': details.get('all_time_quantity_sold', 0)
            }, crawl_timestamp)
            
            # Insert rating history with crawl_timestamp
            db.insert_rating_history(product_id, details, crawl_timestamp)
            
            # Insert product details with crawl_timestamp
            db.insert_product_details(product_id, details, crawl_timestamp)
            
            successful += 1
            
            if successful % 50 == 0:
                logger.info(f"  Stored {successful}/{len(all_products)} products, {len(sellers_stored)} unique sellers...")
                
        except Exception as e:
            logger.error(f"Failed to store product {product.get('product_id')}: {e}")
            failed += 1
    
    logger.info(f"Storage complete: {successful} successful, {failed} failed, {len(sellers_stored)} unique sellers")
    return successful, failed


def run_parallel_crawl(config: dict, logger):
    """Run parallel multi-category crawl"""
    logger.info("=" * 70)
    logger.info("STARTING PARALLEL MULTI-CATEGORY CRAWL")
    logger.info("=" * 70)
    
    start_time = datetime.now()
    
    # Initialize components
    crawler = TikiParallelCrawler(config)
    db_path = config.get('database', {}).get('db_path', 'data/database/tiki_products_multi.db')
    db = DatabaseV2(db_path)
    
    try:
        # Crawl all categories with parallel workers
        results = crawler.crawl_all_categories_parallel()
        
        # Save raw results
        data_dir = Path(config.get('database', {}).get('data_dir', 'data/raw'))
        json_file = save_crawl_data(results, data_dir)
        logger.info(f"Raw results saved to: {json_file}")
        
        # Store in database
        stored_success, stored_failed = process_and_store_products(results, db, logger)
        
        # Log crawl session
        end_time = datetime.now()
        categories_crawled = [
            {'id': cat['category_id'], 'name': cat['category_name'], 
             'products': cat['stats']['successful']}
            for cat in results.get('categories', [])
        ]
        
        log_id = db.log_crawl({
            'crawl_type': 'parallel_multi_category',
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'products_count': stored_success,
            'errors_count': stored_failed,
            'status': 'completed',
            'categories_crawled': categories_crawled,
            'parallel_workers': config.get('crawler', {}).get('max_workers', 10),
            'overall_speed': results.get('overall_speed', 0)
        })
        
        # Summary
        duration = (end_time - start_time).total_seconds()
        logger.info("=" * 70)
        logger.info("PARALLEL CRAWL COMPLETED")
        logger.info(f"Duration: {duration/60:.1f} minutes ({duration:.1f} seconds)")
        logger.info(f"Categories: {results['stats']['total_categories']}")
        logger.info(f"Products crawled: {results['stats']['successful_products']}/{results['stats']['total_products']}")
        logger.info(f"Products stored: {stored_success}")
        logger.info(f"Overall speed: {results.get('overall_speed', 0):.2f} products/second")
        logger.info(f"Database: {db_path}")
        logger.info(f"Crawl log ID: {log_id}")
        logger.info("=" * 70)
        
    except KeyboardInterrupt:
        logger.warning("Crawl interrupted by user")
        db.log_crawl({
            'crawl_type': 'parallel_multi_category',
            'start_time': start_time.isoformat(),
            'end_time': datetime.now().isoformat(),
            'status': 'interrupted',
            'error_message': 'User interrupted'
        })
    except Exception as e:
        logger.error(f"Crawl failed: {e}", exc_info=True)
        db.log_crawl({
            'crawl_type': 'parallel_multi_category',
            'start_time': start_time.isoformat(),
            'end_time': datetime.now().isoformat(),
            'status': 'failed',
            'error_message': str(e)
        })
    finally:
        db.close()


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
