"""
Updated database module (V2) - Optimized schema
- Removed: inventory_status, ratings_distribution, reviews table, favourite_count, promotions, seller_rating, seller_review_count
- Added: sellers table with follower info
- Kept: review_count, rating_average
- Modified: brand field supports both brand and authors
"""

import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class DatabaseV2:
    """Updated database with optimized schema"""
    
    def __init__(self, db_path: str):
        """Initialize database"""
        self.db_path = db_path
        self.conn = None
        self.connect()
        self.create_tables()
    
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def create_tables(self):
        """Create optimized tables"""
        cursor = self.conn.cursor()
        
        # Products table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                short_description TEXT,
                url_key TEXT,
                category_id INTEGER,
                category_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Price history
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                price REAL NOT NULL,
                original_price REAL,
                discount REAL,
                discount_rate INTEGER,
                crawl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        """)
        
        # Sales history (REMOVED inventory_status)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sales_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                quantity_sold INTEGER NOT NULL,
                all_time_quantity_sold INTEGER,
                crawl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        """)
        
        # Rating history (REMOVED ratings_distribution, favourite_count, KEPT review_count)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rating_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                rating_average REAL,
                review_count INTEGER,
                crawl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id)
            )
        """)
        
        # Product details (REMOVED promotions, seller_rating, seller_review_count)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS product_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                brand TEXT,
                categories TEXT,
                specifications TEXT,
                badges TEXT,
                seller_id INTEGER,
                crawl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (product_id) REFERENCES products(id),
                FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
            )
        """)
        
        # Sellers table (NEW)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sellers (
                seller_id INTEGER PRIMARY KEY,
                seller_name TEXT,
                seller_url TEXT,
                seller_total_follower INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Crawl logs
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crawl_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                crawl_type TEXT NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                products_count INTEGER DEFAULT 0,
                errors_count INTEGER DEFAULT 0,
                status TEXT,
                error_message TEXT,
                categories_crawled TEXT
            )
        """)
        
        # Create indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_price_history_product 
            ON price_history(product_id, crawl_timestamp)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_sales_history_product 
            ON sales_history(product_id, crawl_timestamp)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_products_category 
            ON products(category_id)
        """)
        
        self.conn.commit()
        logger.info("Database tables created successfully (V2 schema)")
    
    def insert_product(self, product_data: Dict[str, Any], category_id: int = None, 
                      category_name: str = None) -> bool:
        """Insert or update product"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO products 
                (id, name, short_description, url_key, category_id, category_name)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                product_data.get('id'),
                product_data.get('name'),
                product_data.get('short_description'),
                product_data.get('url_key'),
                category_id,
                category_name
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to insert product: {e}")
            return False
    
    def insert_price_history(self, product_id: int, price_data: Dict[str, Any], 
                            crawl_timestamp: Optional[str] = None) -> bool:
        """Insert price history"""
        try:
            cursor = self.conn.cursor()
            if crawl_timestamp:
                cursor.execute("""
                    INSERT INTO price_history 
                    (product_id, price, original_price, discount, discount_rate, crawl_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    product_id,
                    price_data.get('price'),
                    price_data.get('original_price'),
                    price_data.get('discount'),
                    price_data.get('discount_rate'),
                    crawl_timestamp
                ))
            else:
                cursor.execute("""
                    INSERT INTO price_history 
                    (product_id, price, original_price, discount, discount_rate)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    product_id,
                    price_data.get('price'),
                    price_data.get('original_price'),
                    price_data.get('discount'),
                    price_data.get('discount_rate')
                ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to insert price history: {e}")
            return False
    
    def insert_sales_history(self, product_id: int, sales_data: Dict[str, Any],
                            crawl_timestamp: Optional[str] = None) -> bool:
        """Insert sales history (NO inventory_status)"""
        try:
            cursor = self.conn.cursor()
            if crawl_timestamp:
                cursor.execute("""
                    INSERT INTO sales_history 
                    (product_id, quantity_sold, all_time_quantity_sold, crawl_timestamp)
                    VALUES (?, ?, ?, ?)
                """, (
                    product_id,
                    sales_data.get('quantity_sold', 0),
                    sales_data.get('all_time_quantity_sold'),
                    crawl_timestamp
                ))
            else:
                cursor.execute("""
                    INSERT INTO sales_history 
                    (product_id, quantity_sold, all_time_quantity_sold)
                    VALUES (?, ?, ?)
                """, (
                    product_id,
                    sales_data.get('quantity_sold', 0),
                    sales_data.get('all_time_quantity_sold')
                ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to insert sales history: {e}")
            return False
    
    def insert_rating_history(self, product_id: int, rating_data: Dict[str, Any],
                               crawl_timestamp: Optional[str] = None) -> bool:
        """Insert rating history (NO ratings_distribution, favourite_count, YES review_count)"""
        try:
            cursor = self.conn.cursor()
            if crawl_timestamp:
                cursor.execute("""
                    INSERT INTO rating_history 
                    (product_id, rating_average, review_count, crawl_timestamp)
                    VALUES (?, ?, ?, ?)
                """, (
                    product_id,
                    rating_data.get('rating_average'),
                    rating_data.get('review_count'),
                    crawl_timestamp
                ))
            else:
                cursor.execute("""
                    INSERT INTO rating_history 
                    (product_id, rating_average, review_count)
                    VALUES (?, ?, ?)
                """, (
                    product_id,
                    rating_data.get('rating_average'),
                    rating_data.get('review_count')
                ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to insert rating history: {e}")
            return False
    
    def insert_product_details(self, product_id: int, details: Dict[str, Any],
                               crawl_timestamp: Optional[str] = None) -> bool:
        """Insert product details (REMOVED promotions, seller_rating, seller_review_count)"""
        try:
            cursor = self.conn.cursor()
            
            # Extract seller info
            seller_info = details.get('seller_info_enriched', {})
            
            # Handle brand or authors (for books)
            brand_data = details.get('brand')
            if not brand_data or not brand_data.get('name'):
                # Check if it's a book with authors
                authors = details.get('authors')
                if authors:
                    brand_data = {'name': 'Book', 'authors': authors}
            
            # Use badges_v3 instead of badges
            badges_data = details.get('badges_v3') or details.get('badges', [])
            
            if crawl_timestamp:
                cursor.execute("""
                    INSERT INTO product_details 
                    (product_id, brand, categories, specifications, badges, seller_id, crawl_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    product_id,
                    json.dumps(brand_data, ensure_ascii=False) if brand_data else None,
                    json.dumps(details.get('categories', {}), ensure_ascii=False),
                    json.dumps(details.get('specifications', []), ensure_ascii=False),
                    json.dumps(badges_data, ensure_ascii=False),
                    seller_info.get('id'),
                    crawl_timestamp
                ))
            else:
                cursor.execute("""
                    INSERT INTO product_details 
                    (product_id, brand, categories, specifications, badges, seller_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    product_id,
                    json.dumps(brand_data, ensure_ascii=False) if brand_data else None,
                    json.dumps(details.get('categories', {}), ensure_ascii=False),
                    json.dumps(details.get('specifications', []), ensure_ascii=False),
                    json.dumps(badges_data, ensure_ascii=False),
                    seller_info.get('id')
                ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to insert product details: {e}")
            return False
    
    def insert_seller(self, seller_data: Dict[str, Any]) -> bool:
        """Insert or update seller info"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO sellers 
                (seller_id, seller_name, seller_url, seller_total_follower, last_updated)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                seller_data.get('seller_id'),
                seller_data.get('seller_name'),
                seller_data.get('seller_url'),
                seller_data.get('seller_total_follower', 0)
            ))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to insert seller: {e}")
            return False
    
    def log_crawl(self, log_data: Dict[str, Any]) -> int:
        """Log crawl session"""
        try:
            cursor = self.conn.cursor()
            
            categories_crawled = log_data.get('categories_crawled', [])
            if isinstance(categories_crawled, list):
                categories_crawled = json.dumps(categories_crawled)
            
            cursor.execute("""
                INSERT INTO crawl_logs 
                (crawl_type, start_time, end_time, products_count, 
                 errors_count, status, error_message, categories_crawled)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                log_data.get('crawl_type'),
                log_data.get('start_time'),
                log_data.get('end_time'),
                log_data.get('products_count', 0),
                log_data.get('errors_count', 0),
                log_data.get('status'),
                log_data.get('error_message'),
                categories_crawled
            ))
            self.conn.commit()
            return cursor.lastrowid
        except Exception as e:
            logger.error(f"Failed to log crawl: {e}")
            return -1
    
    def get_all_product_ids(self) -> List[int]:
        """Get all product IDs"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM products ORDER BY id")
        return [row[0] for row in cursor.fetchall()]
    
    def get_products_by_category(self, category_id: int) -> List[Dict[str, Any]]:
        """Get products for a category"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM products WHERE category_id = ? ORDER BY id
        """, (category_id,))
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    
    def insert_sellers_batch(self, sellers_batch: List[Dict[str, Any]]):
        """Batch insert sellers"""
        if not sellers_batch:
            return
        try:
            cursor = self.conn.cursor()
            cursor.executemany("""
                INSERT OR REPLACE INTO sellers 
                (seller_id, seller_name, seller_url, seller_total_follower, last_updated)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, [
                (
                    s.get('seller_id'),
                    s.get('seller_name'),
                    s.get('seller_url'),
                    s.get('seller_total_follower', 0)
                )
                for s in sellers_batch
            ])
            self.conn.commit()
        except Exception as e:
            logger.error(f"Failed to batch insert sellers: {e}")
            raise
    
    def insert_products_batch(self, products_batch: List[Dict[str, Any]]):
        """Batch insert products"""
        if not products_batch:
            return
        try:
            cursor = self.conn.cursor()
            cursor.executemany("""
                INSERT OR REPLACE INTO products 
                (id, name, short_description, url_key, category_id, category_name)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [
                (
                    p.get('id'),
                    p.get('name'),
                    p.get('short_description'),
                    p.get('url_key'),
                    p.get('category_id'),
                    p.get('category_name')
                )
                for p in products_batch
            ])
            self.conn.commit()
        except Exception as e:
            logger.error(f"Failed to batch insert products: {e}")
            raise
    
    def insert_price_history_batch(self, price_history_batch: List[Dict[str, Any]]):
        """Batch insert price history"""
        if not price_history_batch:
            return
        try:
            cursor = self.conn.cursor()
            cursor.executemany("""
                INSERT INTO price_history 
                (product_id, price, original_price, discount, discount_rate, crawl_timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [
                (
                    ph.get('product_id'),
                    ph.get('price'),
                    ph.get('original_price'),
                    ph.get('discount'),
                    ph.get('discount_rate'),
                    ph.get('crawl_timestamp')
                )
                for ph in price_history_batch
            ])
            self.conn.commit()
        except Exception as e:
            logger.error(f"Failed to batch insert price history: {e}")
            raise
    
    def insert_sales_history_batch(self, sales_history_batch: List[Dict[str, Any]]):
        """Batch insert sales history"""
        if not sales_history_batch:
            return
        try:
            cursor = self.conn.cursor()
            cursor.executemany("""
                INSERT INTO sales_history 
                (product_id, quantity_sold, all_time_quantity_sold, crawl_timestamp)
                VALUES (?, ?, ?, ?)
            """, [
                (
                    sh.get('product_id'),
                    sh.get('quantity_sold', 0),
                    sh.get('all_time_quantity_sold'),
                    sh.get('crawl_timestamp')
                )
                for sh in sales_history_batch
            ])
            self.conn.commit()
        except Exception as e:
            logger.error(f"Failed to batch insert sales history: {e}")
            raise
    
    def insert_rating_history_batch(self, rating_history_batch: List[Dict[str, Any]]):
        """Batch insert rating history"""
        if not rating_history_batch:
            return
        try:
            cursor = self.conn.cursor()
            cursor.executemany("""
                INSERT INTO rating_history 
                (product_id, rating_average, review_count, crawl_timestamp)
                VALUES (?, ?, ?, ?)
            """, [
                (
                    rh.get('product_id'),
                    rh.get('rating_average'),
                    rh.get('review_count'),
                    rh.get('crawl_timestamp')
                )
                for rh in rating_history_batch
            ])
            self.conn.commit()
        except Exception as e:
            logger.error(f"Failed to batch insert rating history: {e}")
            raise
    
    def insert_product_details_batch(self, product_details_batch: List[Dict[str, Any]]):
        """Batch insert product details"""
        if not product_details_batch:
            return
        try:
            cursor = self.conn.cursor()
            cursor.executemany("""
                INSERT INTO product_details 
                (product_id, brand, categories, specifications, badges, seller_id, crawl_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                (
                    pd.get('product_id'),
                    pd.get('brand'),
                    None,  # categories - not used in current schema
                    None,  # specifications - not used in current schema
                    pd.get('badges'),
                    pd.get('seller_id'),
                    pd.get('crawl_timestamp')
                )
                for pd in product_details_batch
            ])
            self.conn.commit()
        except Exception as e:
            logger.error(f"Failed to batch insert product details: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")



