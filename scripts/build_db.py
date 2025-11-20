"""
Build database from ALL JSON files in data/raw
Import all data from crawl results JSON files (sorted by time, oldest first)
Optimized for performance with batch inserts
"""

import json
import logging
import argparse
from pathlib import Path
from datetime import datetime
import sys
import re

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from crawler.database_v2 import DatabaseV2


def setup_logging():
    """Setup logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def extract_timestamp_from_filename(filename: str) -> datetime:
    """Extract timestamp from filename like parallel_crawl_results_20251121_022421.json"""
    match = re.search(r'(\d{8}_\d{6})', filename)
    if match:
        try:
            return datetime.strptime(match.group(1), '%Y%m%d_%H%M%S')
        except:
            pass
    # Fallback to file modification time
    return datetime.fromtimestamp(Path(filename).stat().st_mtime)


def get_sorted_json_files(raw_dir: Path) -> list:
    """Get all JSON files sorted by timestamp (oldest first)"""
    json_files = list(raw_dir.glob('parallel_crawl_results_*.json'))
    # Sort by timestamp extracted from filename
    json_files.sort(key=lambda p: extract_timestamp_from_filename(p.name))
    return json_files


def load_json_file(json_path: Path) -> dict:
    """Load JSON file"""
    logger = logging.getLogger(__name__)
    try:
        logger.info(f"Loading {json_path.name}...")
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        product_count = len(data.get('all_products', []))
        logger.info(f"  ✓ Loaded {json_path.name}: {product_count} products")
        return data
    except Exception as e:
        logger.error(f"  ✗ Failed to load {json_path.name}: {e}")
        raise


def process_and_store_products_batch(results: dict, db: DatabaseV2, logger, 
                                     existing_products: set, existing_sellers: set):
    """Process crawl results and store in database with batch operations"""
    all_products = results.get('all_products', [])
    successful = 0
    failed = 0
    
    # Get start_time from crawl results to use as crawl_timestamp
    crawl_timestamp = results.get('start_time')
    
    # Prepare batch data
    products_batch = []
    sellers_batch = []
    price_history_batch = []
    sales_history_batch = []
    rating_history_batch = []
    product_details_batch = []
    
    new_sellers = set()
    
    for product in all_products:
        if not product.get('success'):
            failed += 1
            continue
        
        try:
            details = product.get('details', {})
            product_id = details.get('id')
            category_id = product.get('category_id')
            category_name = product.get('category_name')
            
            # Collect seller info
            seller_info = details.get('seller_info_enriched', {})
            seller_id = seller_info.get('id')
            if seller_id and seller_id not in existing_sellers and seller_id not in new_sellers:
                sellers_batch.append({
                    'seller_id': seller_id,
                    'seller_name': seller_info.get('name'),
                    'seller_url': seller_info.get('link'),
                    'seller_total_follower': seller_info.get('total_follower')
                })
                new_sellers.add(seller_id)
            
            # Collect product data
            products_batch.append({
                'id': product_id,
                'name': details.get('name'),
                'short_description': details.get('short_description'),
                'url_key': details.get('url_key'),
                'category_id': category_id,
                'category_name': category_name
            })
            
            # Collect history data
            price_history_batch.append({
                'product_id': product_id,
                'price': details.get('price'),
                'original_price': details.get('original_price'),
                'discount': details.get('discount'),
                'discount_rate': details.get('discount_rate'),
                'crawl_timestamp': crawl_timestamp
            })
            
            sales_history_batch.append({
                'product_id': product_id,
                'quantity_sold': details.get('quantity_sold', {}).get('value', 0),
                'all_time_quantity_sold': details.get('all_time_quantity_sold', 0),
                'crawl_timestamp': crawl_timestamp
            })
            
            rating_history_batch.append({
                'product_id': product_id,
                'rating_average': details.get('rating_average'),
                'review_count': details.get('review_count'),
                'crawl_timestamp': crawl_timestamp
            })
            
            # Product details
            brand = details.get('brand')
            badges = details.get('badges_v3') or details.get('badges', [])
            
            product_details_batch.append({
                'product_id': product_id,
                'brand': json.dumps(brand, ensure_ascii=False) if brand else None,
                'badges': json.dumps(badges, ensure_ascii=False) if badges else None,
                'seller_id': seller_id,
                'crawl_timestamp': crawl_timestamp
            })
            
            existing_products.add(product_id)
            successful += 1
                
        except Exception as e:
            logger.error(f"Failed to process product {product.get('product_id')}: {e}")
            failed += 1
    
    # Batch insert
    try:
        if sellers_batch:
            db.insert_sellers_batch(sellers_batch)
            existing_sellers.update(new_sellers)
        
        if products_batch:
            db.insert_products_batch(products_batch)
        
        if price_history_batch:
            db.insert_price_history_batch(price_history_batch)
        
        if sales_history_batch:
            db.insert_sales_history_batch(sales_history_batch)
        
        if rating_history_batch:
            db.insert_rating_history_batch(rating_history_batch)
        
        if product_details_batch:
            db.insert_product_details_batch(product_details_batch)
        
        logger.info(f"  Batch inserted: {successful} products, {len(new_sellers)} sellers")
        
    except Exception as e:
        logger.error(f"Batch insert failed: {e}")
        raise
    
    return successful, failed


def build_database_from_all_json(json_files: list, db_path: str, logger):
    """Build database from all JSON files"""
    logger.info("=" * 70)
    logger.info("BUILDING DATABASE FROM ALL JSON FILES")
    logger.info("=" * 70)
    logger.info(f"Found {len(json_files)} JSON files")
    logger.info(f"Database: {db_path}")
    logger.info("Processing files in chronological order (oldest first)...")
    logger.info("")
    
    # Initialize database
    db = DatabaseV2(db_path)
    
    # Track existing data to avoid duplicates
    existing_products = set()
    existing_sellers = set()
    
    total_successful = 0
    total_failed = 0
    
    try:
        for idx, json_file in enumerate(json_files, 1):
            logger.info(f"[{idx}/{len(json_files)}] Processing {json_file.name}...")
            
            try:
                # Load JSON data
                results = load_json_file(json_file)
                
                # Process and store products
                successful, failed = process_and_store_products_batch(
                    results, db, logger, existing_products, existing_sellers
                )
                
                total_successful += successful
                total_failed += failed
                
                logger.info(f"  ✓ Completed: {successful} products stored, {failed} failed")
                
            except Exception as e:
                logger.error(f"  ✗ Failed to process {json_file.name}: {e}")
                continue
        
        # Summary
        logger.info("")
        logger.info("=" * 70)
        logger.info("DATABASE BUILD COMPLETED")
        logger.info(f"Total files processed: {len(json_files)}")
        logger.info(f"Total products stored: {total_successful}")
        logger.info(f"Total products failed: {total_failed}")
        logger.info(f"Unique products: {len(existing_products)}")
        logger.info(f"Unique sellers: {len(existing_sellers)}")
        logger.info(f"Database: {db_path}")
        logger.info("=" * 70)
        
    except Exception as e:
        logger.error(f"Build failed: {e}", exc_info=True)
        raise
    finally:
        db.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Build database from ALL JSON files in data/raw')
    parser.add_argument('--db', default='data/database/tiki_products_multi.db', 
                       help='Path to database file')
    parser.add_argument('--raw-dir', default='data/raw',
                       help='Directory containing JSON files')
    
    args = parser.parse_args()
    
    logger = setup_logging()
    
    # Get all JSON files sorted by time (oldest first)
    raw_dir = Path(args.raw_dir)
    json_files = get_sorted_json_files(raw_dir)
    
    if not json_files:
        logger.error(f"No JSON files found in {raw_dir}")
        sys.exit(1)
    
    logger.info(f"Found {len(json_files)} JSON files to process")
    
    # Ensure database directory exists
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Build database from all files
    build_database_from_all_json(json_files, str(db_path), logger)


if __name__ == '__main__':
    main()
