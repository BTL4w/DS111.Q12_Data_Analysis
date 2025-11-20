"""
Export database to CSV files
Easy-to-use wrapper for CSVExporterV2
"""

import sys
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from utils.export_csv_v2 import CSVExporterV2


def main():
    """Export all data to CSV"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    # Configuration
    db_path = 'data/database/tiki_products_multi.db'
    output_dir = 'data/exports'
    
    logger.info("=" * 70)
    logger.info("EXPORTING DATABASE TO CSV")
    logger.info("=" * 70)
    logger.info(f"Database: {db_path}")
    logger.info(f"Output: {output_dir}")
    logger.info("")
    
    try:
        # Create exporter
        exporter = CSVExporterV2(db_path, output_dir)
        
        # Export all tables
        results = exporter.export_all()
        
        # Summary
        logger.info("")
        logger.info("=" * 70)
        logger.info("EXPORT COMPLETED")
        logger.info("=" * 70)
        logger.info(f"Total files: {len(results)}")
        logger.info("")
        
        for name, filepath in results.items():
            logger.info(f"✓ {name:20s} -> {filepath}")
        
        logger.info("")
        logger.info(f"All files saved to: {output_dir}/")
        logger.info("=" * 70)
        
    except FileNotFoundError:
        logger.error(f"Database not found: {db_path}")
        logger.error("Please run the crawler first to create the database.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Export failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()


