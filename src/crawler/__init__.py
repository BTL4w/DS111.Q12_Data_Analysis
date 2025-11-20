"""
Crawler module for collecting product data from Tiki
"""

from .crawler_parallel import TikiParallelCrawler
from .database_v2 import DatabaseV2

__all__ = ['TikiParallelCrawler', 'DatabaseV2']

