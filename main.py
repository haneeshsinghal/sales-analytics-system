import os
import subprocess
import sys
from pathlib import Path
import datetime
from collections import defaultdict, Counter

# --------------------------
# Utility Functions
# --------------------------
# Find project root
def find_project_root(preferred_dirs=None, max_up=2):
    """Find the project root directory by looking for specific marker files or directories."""
    preferred_dirs = preferred_dirs or ['data', 'utils', 'output', 'logs']
    current_dir = Path.cwd().resolve()
    for hop in range(max_up + 1):
        candidate = current_dir
        for _ in range(hop):
            candidate = candidate.parent
        if any((candidate / d).exists() for d in preferred_dirs):
            return candidate
    return candidate

# Ensure directories exist
def ensure_dirs(dirs):
    """Ensure that the specified directories exist."""
    for directory in dirs:
        Path(directory).mkdir(parents=True, exist_ok=True)

# Install required packages
def install_package(package_file):
    """Install a package using pip dynamically."""
    if os.path.exists(package_file):
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', "-r", str(package_file)])

import logging

# ----------- Setup logging -----------
# Configuring logging to display messages to the console

# Setup logging
def setup_logging(log_file_name, logs_level = logging.INFO):
    logging.basicConfig(    
        filename=str(log_file_name),
        level=logs_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        encoding='utf-8'
    )


# ------------------
# Main function
# ------------------

def main():          

    # Find project root
    project_root = find_project_root()
    input_data_dir = project_root / 'data'
    output_dir = project_root / 'output'
    logs_dir = project_root / 'logs'
    utils_dir = project_root / 'utils'

    log_file_name = 'sales_analysis.log'
    package_file_name = 'requirements.txt'
    input_data_filename = 'sales_data.txt'
    enriched_sales_data_filename = 'enriched_sales_data.txt'
    report_file_name = 'sales_report.txt'

    required_library_path = project_root / package_file_name
    input_data_path = input_data_dir / input_data_filename
    log_file_path = logs_dir / log_file_name
    enriched_sales_data_path = input_data_dir / enriched_sales_data_filename
    report_file_path = output_dir / report_file_name

    # Ensure necessary directories exist
    ensure_dirs([input_data_dir, output_dir, logs_dir, utils_dir])

    # Installing required packages
    install_package(required_library_path)

    # Setup logging
    setup_logging(str(log_file_path))
    # logger Object
    logger = logging. getLogger()

    logger.info(f"="*40)
    logger.info("SALES ANALYTICS SYSTEM")
    logger.info(f"="*40 + "\n")
    
    print(f"\n" + "="*40)
    print("SALES ANALYTICS SYSTEM")
    print(f"="*40 + "\n")

    logger.info(f"-"*75)
    logger.info("Part 1: Data File Handler & Preprocessing (File I/O & Error Handling)")
    logger.info(f"-"*75)    

    from utils.file_handler import SalesDataFileHandler

    file_handler = SalesDataFileHandler(logger = logger)

    # Read sales data
    logger.info("[1/10] Reading sales data...")
    print("[1/10] Reading sales data...")

    # Read sales data from file using File Handler
    read_sales_data, encoding_used = file_handler.read_sales_data(str(input_data_path))

    logger.info("Read %d data lines from %s using %s encoding.", len(read_sales_data), str(input_data_path.name), encoding_used)
    logger.info("First 5 data lines: %s", read_sales_data[:5])
    logger.info(f"✓ Successfully read {len(read_sales_data)} transactions\n")
    print(f"✓ Successfully read {len(read_sales_data)} transactions\n")

    # Process sales data
    logger.info(f"-"*75)
    logger.info("[2/10] Parsing and cleaning data...")
    logger.info(f"-"*75)
    print("[2/10] Parsing and cleaning data...\n")

    # Parse sales data using File Handler
    parsed_sales_data = file_handler.parse_transactions(read_sales_data)
    
    logger.info("First 5 parsed transactions: %s", parsed_sales_data[:5])
    logger.info(f"✓ Parsed {len(read_sales_data)} records\n")
    print(f"\n✓ Parsed {len(read_sales_data)} records\n")

    

# Run main function
if __name__ == "__main__":
    main()