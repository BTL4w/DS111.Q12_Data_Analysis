"""
Export data from DatabaseV2 to CSV files
Compatible with updated schema (no favourite_count, inventory_status, reviews)
"""

import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
import logging
import sqlite3

logger = logging.getLogger(__name__)


class CSVExporterV2:
    """Export DatabaseV2 data to CSV files"""
    
    def __init__(self, db_path: str, output_dir: str = "data/exports"):
        """
        Initialize CSV exporter
        
        Args:
            db_path: Path to SQLite database
            output_dir: Directory to save CSV files
        """
        self.db_path = db_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"CSVExporterV2 initialized: {db_path} -> {output_dir}")
    
    def _get_connection(self):
        """Get database connection"""
        return sqlite3.connect(self.db_path)
    
    def export_products(self, filename: Optional[str] = None) -> str:
        """Export products table to CSV"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"products_{timestamp}.csv"
        
        output_path = self.output_dir / filename
        
        try:
            conn = self._get_connection()
            query = """
                SELECT 
                    id,
                    name,
                    short_description,
                    url_key,
                    category_id,
                    category_name,
                    created_at
                FROM products
                ORDER BY category_id, id
            """
            
            df = pd.read_sql_query(query, conn)
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            conn.close()
            
            logger.info(f"Exported {len(df)} products to {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Failed to export products: {e}")
            raise
    
    def export_price_history(self, filename: Optional[str] = None) -> str:
        """Export price history to CSV"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"price_history_{timestamp}.csv"
        
        output_path = self.output_dir / filename
        
        try:
            conn = self._get_connection()
            query = """
                SELECT 
                    ph.id,
                    ph.product_id,
                    p.name as product_name,
                    p.category_name,
                    ph.price,
                    ph.original_price,
                    ph.discount,
                    ph.discount_rate,
                    ph.crawl_timestamp
                FROM price_history ph
                JOIN products p ON ph.product_id = p.id
                ORDER BY ph.crawl_timestamp, ph.product_id
            """
            
            df = pd.read_sql_query(query, conn)
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            conn.close()
            
            logger.info(f"Exported {len(df)} price records to {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Failed to export price history: {e}")
            raise
    
    def export_sales_history(self, filename: Optional[str] = None) -> str:
        """Export sales history to CSV"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"sales_history_{timestamp}.csv"
        
        output_path = self.output_dir / filename
        
        try:
            conn = self._get_connection()
            query = """
                SELECT 
                    sh.id,
                    sh.product_id,
                    p.name as product_name,
                    p.category_name,
                    sh.quantity_sold,
                    sh.all_time_quantity_sold,
                    sh.crawl_timestamp
                FROM sales_history sh
                JOIN products p ON sh.product_id = p.id
                ORDER BY sh.crawl_timestamp, sh.product_id
            """
            
            df = pd.read_sql_query(query, conn)
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            conn.close()
            
            logger.info(f"Exported {len(df)} sales records to {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Failed to export sales history: {e}")
            raise
    
    def export_rating_history(self, filename: Optional[str] = None) -> str:
        """Export rating history to CSV"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"rating_history_{timestamp}.csv"
        
        output_path = self.output_dir / filename
        
        try:
            conn = self._get_connection()
            query = """
                SELECT 
                    rh.id,
                    rh.product_id,
                    p.name as product_name,
                    p.category_name,
                    rh.rating_average,
                    rh.review_count,
                    rh.crawl_timestamp
                FROM rating_history rh
                JOIN products p ON rh.product_id = p.id
                ORDER BY rh.crawl_timestamp, rh.product_id
            """
            
            df = pd.read_sql_query(query, conn)
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            conn.close()
            
            logger.info(f"Exported {len(df)} rating records to {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Failed to export rating history: {e}")
            raise
    
    def export_sellers(self, filename: Optional[str] = None) -> str:
        """Export sellers table to CSV"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"sellers_{timestamp}.csv"
        
        output_path = self.output_dir / filename
        
        try:
            conn = self._get_connection()
            query = """
                SELECT 
                    seller_id,
                    seller_name,
                    seller_url,
                    seller_total_follower,
                    last_updated
                FROM sellers
                ORDER BY seller_total_follower DESC
            """
            
            df = pd.read_sql_query(query, conn)
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            conn.close()
            
            logger.info(f"Exported {len(df)} sellers to {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Failed to export sellers: {e}")
            raise
    
    def export_product_details(self, filename: Optional[str] = None) -> str:
        """Export product details with parsed JSON fields"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"product_details_{timestamp}.csv"
        
        output_path = self.output_dir / filename
        
        try:
            conn = self._get_connection()
            query = """
                SELECT 
                    pd.id,
                    pd.product_id,
                    p.name as product_name,
                    p.category_name,
                    pd.brand,
                    pd.categories,
                    pd.specifications,
                    pd.badges,
                    pd.seller_id,
                    s.seller_name,
                    s.seller_total_follower,
                    pd.crawl_timestamp
                FROM product_details pd
                JOIN products p ON pd.product_id = p.id
                LEFT JOIN sellers s ON pd.seller_id = s.seller_id
                ORDER BY pd.product_id
            """
            
            df = pd.read_sql_query(query, conn)
            
            # Parse JSON columns to extract key info
            if not df.empty:
                # Extract brand name
                df['brand_name'] = df['brand'].apply(
                    lambda x: json.loads(x).get('name', '') if x and pd.notna(x) else ''
                )
                
                # Count specifications
                df['spec_count'] = df['specifications'].apply(
                    lambda x: len(json.loads(x)) if x and pd.notna(x) else 0
                )
                
                # Count badges
                df['badge_count'] = df['badges'].apply(
                    lambda x: len(json.loads(x)) if x and pd.notna(x) else 0
                )
                
                # Extract badge names
                df['badge_names'] = df['badges'].apply(
                    lambda x: ', '.join([b.get('name', '') for b in json.loads(x)]) if x and pd.notna(x) else ''
                )
            
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            conn.close()
            
            logger.info(f"Exported {len(df)} product details to {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Failed to export product details: {e}")
            raise
    
    def export_latest_snapshot(self, filename: Optional[str] = None) -> str:
        """
        Export latest snapshot with all metrics joined
        This is the main file for analysis
        """
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"products_latest_snapshot_{timestamp}.csv"
        
        output_path = self.output_dir / filename
        
        try:
            conn = self._get_connection()
            query = """
                SELECT 
                    p.id as product_id,
                    p.name as product_name,
                    p.short_description,
                    p.url_key,
                    p.category_id,
                    p.category_name,
                    
                    -- Latest price info
                    ph.price as current_price,
                    ph.original_price,
                    ph.discount,
                    ph.discount_rate,
                    
                    -- Latest sales info
                    sh.quantity_sold,
                    sh.all_time_quantity_sold,
                    
                    -- Latest rating info
                    rh.rating_average,
                    rh.review_count,
                    
                    -- Product details
                    pd.brand,
                    pd.badges,
                    
                    -- Seller info
                    pd.seller_id,
                    s.seller_name,
                    s.seller_total_follower,
                    
                    -- Timestamps
                    ph.crawl_timestamp as last_price_update,
                    sh.crawl_timestamp as last_sales_update,
                    rh.crawl_timestamp as last_rating_update
                    
                FROM products p
                
                LEFT JOIN (
                    SELECT product_id, price, original_price, discount, discount_rate, crawl_timestamp
                    FROM price_history
                    WHERE (product_id, crawl_timestamp) IN (
                        SELECT product_id, MAX(crawl_timestamp)
                        FROM price_history
                        GROUP BY product_id
                    )
                ) ph ON p.id = ph.product_id
                
                LEFT JOIN (
                    SELECT product_id, quantity_sold, all_time_quantity_sold, crawl_timestamp
                    FROM sales_history
                    WHERE (product_id, crawl_timestamp) IN (
                        SELECT product_id, MAX(crawl_timestamp)
                        FROM sales_history
                        GROUP BY product_id
                    )
                ) sh ON p.id = sh.product_id
                
                LEFT JOIN (
                    SELECT product_id, rating_average, review_count, crawl_timestamp
                    FROM rating_history
                    WHERE (product_id, crawl_timestamp) IN (
                        SELECT product_id, MAX(crawl_timestamp)
                        FROM rating_history
                        GROUP BY product_id
                    )
                ) rh ON p.id = rh.product_id
                
                LEFT JOIN product_details pd ON p.id = pd.product_id
                LEFT JOIN sellers s ON pd.seller_id = s.seller_id
                
                ORDER BY p.category_id, p.id
            """
            
            df = pd.read_sql_query(query, conn)
            
            # Parse JSON columns
            if not df.empty:
                # Extract brand name
                df['brand_name'] = df['brand'].apply(
                    lambda x: json.loads(x).get('name', '') if pd.notna(x) and x else ''
                )
                
                # Extract authors for books
                df['authors'] = df['brand'].apply(
                    lambda x: ', '.join([a.get('name', '') for a in json.loads(x).get('authors', [])]) 
                    if pd.notna(x) and x and 'authors' in json.loads(x) else ''
                )
                
                # Extract badge names
                df['badge_names'] = df['badges'].apply(
                    lambda x: ', '.join([b.get('name', '') for b in json.loads(x)]) 
                    if pd.notna(x) and x else ''
                )
                
                # Drop JSON columns to keep CSV clean
                df = df.drop(columns=['brand', 'badges'], errors='ignore')
                
                # Calculate derived metrics
                df['price_drop_amount'] = df['original_price'] - df['current_price']
                df['has_discount'] = df['discount_rate'] > 0
                if len(df) > 0:
                    df['is_bestseller'] = df['all_time_quantity_sold'] > df['all_time_quantity_sold'].quantile(0.75)
                df['high_rating'] = df['rating_average'] >= 4.5
            
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            conn.close()
            
            logger.info(f"Exported latest snapshot: {len(df)} products to {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Failed to export latest snapshot: {e}")
            raise
    
    def export_all(self) -> Dict[str, str]:
        """
        Export all tables to CSV
        
        Returns:
            Dictionary mapping table name to file path
        """
        logger.info("Starting export of all tables...")
        
        results = {}
        
        exports = [
            ('products', self.export_products),
            ('price_history', self.export_price_history),
            ('sales_history', self.export_sales_history),
            ('rating_history', self.export_rating_history),
            ('sellers', self.export_sellers),
            ('product_details', self.export_product_details),
            ('latest_snapshot', self.export_latest_snapshot)
        ]
        
        for name, export_func in exports:
            try:
                filepath = export_func()
                results[name] = filepath
                logger.info(f"✓ {name}: {filepath}")
            except Exception as e:
                logger.error(f"✗ {name}: {e}")
        
        logger.info(f"Export completed: {len(results)}/{len(exports)} files created")
        return results


def main():
    """Command-line interface for CSV export"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Export DatabaseV2 to CSV files')
    parser.add_argument('--db', default='data/tiki_products_multi.db', 
                       help='Path to database file')
    parser.add_argument('--output', default='data/exports',
                       help='Output directory for CSV files')
    parser.add_argument('--table', choices=['all', 'products', 'price_history', 
                       'sales_history', 'rating_history', 'sellers', 
                       'product_details', 'snapshot'],
                       default='all', help='Which table to export')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    exporter = CSVExporterV2(args.db, args.output)
    
    if args.table == 'all':
        results = exporter.export_all()
        print(f"\n✓ Exported {len(results)} files to {args.output}")
        for name, path in results.items():
            print(f"  - {name}: {path}")
    else:
        export_map = {
            'products': exporter.export_products,
            'price_history': exporter.export_price_history,
            'sales_history': exporter.export_sales_history,
            'rating_history': exporter.export_rating_history,
            'sellers': exporter.export_sellers,
            'product_details': exporter.export_product_details,
            'snapshot': exporter.export_latest_snapshot
        }
        
        filepath = export_map[args.table]()
        print(f"\n✓ Exported to: {filepath}")


if __name__ == '__main__':
    main()

