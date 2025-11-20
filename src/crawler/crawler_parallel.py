"""
Parallel Multi-category crawler for Tiki (FAST VERSION)
Uses ThreadPoolExecutor for concurrent crawling
"""

import requests
import json
import time
import random
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

logger = logging.getLogger(__name__)


class TikiParallelCrawler:
    """Parallel crawler for multiple categories with concurrent requests"""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize parallel crawler"""
        self.config = config
        self.api_config = config.get('api', {})
        self.crawler_config = config.get('crawler', {})
        self.user_agents = self.crawler_config.get('user_agents', [])
        self.request_delay = self.api_config.get('request_delay', 1)
        self.max_retries = self.api_config.get('max_retries', 3)
        self.timeout = self.api_config.get('timeout', 30)
        
        # Parallel config
        self.max_workers = self.crawler_config.get('max_workers', 10)  # Number of parallel workers
        self.rate_limit_per_second = self.crawler_config.get('rate_limit_per_second', 5)  # Max requests per second
        
        # Thread-safe session and counters
        self.session = requests.Session()
        self.stats_lock = Lock()
        self.request_times = []  # Track request times for rate limiting
        self.request_times_lock = Lock()
        
        self.load_categories()
        
        logger.info(f"TikiParallelCrawler initialized with {self.max_workers} workers")
    
    def load_categories(self):
        """Load categories from config file"""
        categories_file = self.crawler_config.get('categories_file', 'config/categories.json')
        try:
            with open(categories_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.categories = data.get('categories', [])
            logger.info(f"Loaded {len(self.categories)} categories")
        except Exception as e:
            logger.error(f"Failed to load categories: {e}")
            self.categories = []
    
    def _get_headers(self) -> Dict[str, str]:
        """Get random headers"""
        user_agent = random.choice(self.user_agents) if self.user_agents else \
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        
        return {
            'User-Agent': user_agent,
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://tiki.vn/'
        }
    
    def _rate_limit(self):
        """Smart rate limiting - ensure we don't exceed rate limit"""
        with self.request_times_lock:
            current_time = time.time()
            
            # Remove old request times (older than 1 second)
            self.request_times = [t for t in self.request_times if current_time - t < 1.0]
            
            # If we've hit the rate limit, wait
            if len(self.request_times) >= self.rate_limit_per_second:
                sleep_time = 1.0 - (current_time - self.request_times[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                # Clean old times again
                current_time = time.time()
                self.request_times = [t for t in self.request_times if current_time - t < 1.0]
            
            # Record this request
            self.request_times.append(time.time())
    
    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[requests.Response]:
        """Make HTTP request with retry logic and rate limiting"""
        for attempt in range(self.max_retries):
            try:
                # Rate limiting
                self._rate_limit()
                
                # Small random delay to spread requests
                time.sleep(random.uniform(0.1, 0.3))
                
                response = self.session.get(
                    url,
                    headers=self._get_headers(),
                    params=params,
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    return response
                elif response.status_code == 429:
                    wait_time = (2 ** attempt) * self.request_delay
                    logger.warning(f"Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.warning(f"Request failed with status {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error (attempt {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    wait_time = (2 ** attempt) * self.request_delay
                    time.sleep(wait_time)
        
        return None
    
    def get_product_ids_for_category(self, category_id: int, max_products: int = 250) -> List[str]:
        """
        Crawl product IDs for a specific category
        
        Args:
            category_id: Tiki category ID
            max_products: Maximum number of products to crawl
            
        Returns:
            List of product IDs
        """
        product_ids = []
        page = 1
        listing_api = self.api_config.get('listing_api', 'https://tiki.vn/api/v2/products')
        products_per_page = self.crawler_config.get('products_per_page', 48)
        
        logger.info(f"Crawling category {category_id}, max {max_products} products...")
        
        while len(product_ids) < max_products:
            params = {
                'limit': products_per_page,
                'category': category_id,
                'page': page,
                'aggregations': 2
            }
            
            response = self._make_request(listing_api, params=params)
            
            if not response:
                logger.error(f"Failed to fetch page {page}")
                break
            
            try:
                data = response.json()
                products = data.get('data', [])
                
                if not products:
                    break
                
                for product in products:
                    if len(product_ids) >= max_products:
                        break
                    
                    product_id = str(product.get('id'))
                    if product_id:
                        product_ids.append(product_id)
                
                # Check pagination
                paging = data.get('paging', {})
                current_page = paging.get('current_page', page)
                last_page = paging.get('last_page', page)
                
                if current_page >= last_page:
                    break
                
                page += 1
                
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Error parsing page {page}: {e}")
                break
        
        logger.info(f"Collected {len(product_ids)} products for category {category_id}")
        return product_ids
    
    def get_seller_follower_info(self, seller_id: int) -> Optional[int]:
        """Get seller follower count from social API"""
        url = "https://api.tiki.vn/social/openapi/interaction/following"
        params = {'tiki_seller_id': seller_id}
        
        response = self._make_request(url, params=params)
        
        if not response:
            return None
        
        try:
            data = response.json()
            follower_info = data.get('data', {}).get('following', {})
            total_follower = follower_info.get('total_follower', 0)
            return total_follower
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"Failed to parse seller follower info for seller {seller_id}: {e}")
            return None
    
    def get_product_details(self, product_id: str) -> Optional[Dict[str, Any]]:
        """
        Get product details (with seller follower info)
        
        Returns enriched product data
        """
        url = self.api_config.get('product_api', 'https://tiki.vn/api/v2/products/{}').format(product_id)
        response = self._make_request(url)
        
        if not response:
            return None
        
        try:
            data = response.json()
            
            # Extract seller info
            seller_info = data.get('current_seller', {})
            if seller_info:
                seller_id = seller_info.get('id')
                
                # Get seller follower count
                total_follower = None
                if seller_id:
                    total_follower = self.get_seller_follower_info(seller_id)
                
                seller_enriched = {
                    'id': seller_id,
                    'name': seller_info.get('name'),
                    'store_id': seller_info.get('store_id'),
                    'link': seller_info.get('link'),
                    'total_follower': total_follower
                }
                data['seller_info_enriched'] = seller_enriched
            
            return data
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON for product {product_id}: {e}")
            return None
    
    def _crawl_single_product(self, product_id: str, category_id: int, 
                            category_name: str, idx: int, total: int) -> Dict[str, Any]:
        """
        Crawl a single product (worker function for parallel execution)
        
        Args:
            product_id: Product ID to crawl
            category_id: Category ID
            category_name: Category name
            idx: Current index
            total: Total products
            
        Returns:
            Product data dictionary
        """
        logger.info(f"[{idx}/{total}] Crawling product {product_id}...")
        
        details = self.get_product_details(product_id)
        if details:
            return {
                'product_id': product_id,
                'details': details,
                'success': True,
                'category_id': category_id,
                'category_name': category_name
            }
        else:
            return {
                'product_id': product_id,
                'success': False,
                'category_id': category_id,
                'category_name': category_name
            }
    
    def crawl_category_parallel(self, category: Dict[str, Any]) -> Dict[str, Any]:
        """
        Crawl a single category using parallel workers
        
        Returns:
            Dictionary with products and stats
        """
        category_id = category.get('id')
        category_name = category.get('name')
        max_products = self.crawler_config.get('max_products_per_category', 250)
        
        logger.info("=" * 60)
        logger.info(f"CRAWLING CATEGORY: {category_name} (ID: {category_id})")
        logger.info(f"Using {self.max_workers} parallel workers")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        # Get product IDs
        product_ids = self.get_product_ids_for_category(category_id, max_products)
        
        # Crawl products in parallel
        products = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(
                    self._crawl_single_product, 
                    product_id, 
                    category_id, 
                    category_name,
                    idx,
                    len(product_ids)
                ): product_id 
                for idx, product_id in enumerate(product_ids, 1)
            }
            
            # Collect results as they complete
            for future in as_completed(futures):
                try:
                    result = future.result()
                    products.append(result)
                except Exception as e:
                    product_id = futures[future]
                    logger.error(f"Error crawling product {product_id}: {e}")
                    products.append({
                        'product_id': product_id,
                        'success': False,
                        'category_id': category_id,
                        'category_name': category_name,
                        'error': str(e)
                    })
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        result = {
            'category_id': category_id,
            'category_name': category_name,
            'products': products,
            'stats': {
                'total': len(product_ids),
                'successful': len([p for p in products if p['success']]),
                'failed': len([p for p in products if not p['success']]),
                'duration': duration,
                'products_per_second': len(product_ids) / duration if duration > 0 else 0
            }
        }
        
        logger.info(f"Category {category_name} completed in {duration:.1f}s")
        logger.info(f"  Success: {result['stats']['successful']}/{result['stats']['total']}")
        logger.info(f"  Speed: {result['stats']['products_per_second']:.2f} products/second")
        
        return result
    
    def crawl_all_categories_parallel(self) -> Dict[str, Any]:
        """
        Crawl all categories (each category crawled with parallel workers)
        
        Returns:
            Complete results with all products
        """
        start_time = datetime.now()
        
        logger.info("=" * 70)
        logger.info(f"STARTING PARALLEL MULTI-CATEGORY CRAWL - {len(self.categories)} CATEGORIES")
        logger.info(f"Max workers: {self.max_workers}, Rate limit: {self.rate_limit_per_second} req/s")
        logger.info("=" * 70)
        
        all_results = {
            'start_time': start_time.isoformat(),
            'categories': [],
            'all_products': [],
            'stats': {
                'total_categories': len(self.categories),
                'total_products': 0,
                'successful_products': 0,
                'failed_products': 0
            }
        }
        
        for idx, category in enumerate(self.categories, 1):
            logger.info(f"\n[CATEGORY {idx}/{len(self.categories)}]")
            
            result = self.crawl_category_parallel(category)
            all_results['categories'].append(result)
            
            # Aggregate products
            all_results['all_products'].extend(result['products'])
            all_results['stats']['total_products'] += result['stats']['total']
            all_results['stats']['successful_products'] += result['stats']['successful']
            all_results['stats']['failed_products'] += result['stats']['failed']
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        all_results['end_time'] = end_time.isoformat()
        all_results['duration_seconds'] = duration
        all_results['overall_speed'] = all_results['stats']['total_products'] / duration if duration > 0 else 0
        
        logger.info("=" * 70)
        logger.info(f"PARALLEL MULTI-CATEGORY CRAWL COMPLETED in {duration/60:.1f} minutes")
        logger.info(f"Total products: {all_results['stats']['successful_products']}/{all_results['stats']['total_products']}")
        logger.info(f"Overall speed: {all_results['overall_speed']:.2f} products/second")
        logger.info("=" * 70)
        
        return all_results

