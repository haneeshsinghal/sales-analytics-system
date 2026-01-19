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

    # Data Validation and Filtering
    logger.info(f"-"*75)
    logger.info("[3/10] Filter Options Available:")
    logger.info(f"-"*75)
    print("[3/10] Filter Options Available:\n")

    # Display available regions
    regions = sorted(set(txn['Region'] for txn in parsed_sales_data))
    
    logger.info("Regions: %s", ", ".join(regions))
    print("Regions: %s" % ", ".join(regions))

    #Display Amount range
    amounts = [txn['Quantity'] * txn['Price'] for txn in parsed_sales_data]

    logger.info(f"Amount Range: ₹{min(amounts)} - ₹{max(amounts)}")
    print(f"Amount Range: ₹{min(amounts)} - ₹{max(amounts)}\n")    

    # Ask user if they want to apply filters
    apply_filter = input("Do you want to apply filters? (y/n): ").strip().lower()
    
    region_input, min_amount_input, max_amount_input = None, None, None

    if apply_filter == 'y':
        # Get User input for filtering
        region_input = input("Enter region to filter (or press Enter to skip): ").strip()
        min_amount_input = input("Enter minimum amount (or press Enter to skip): ").strip()
        max_amount_input = input("Enter maximum amount (or press Enter to skip): ").strip()

        region = region_input.lower() if region_input.lower() in [r.lower() for r in regions] else None
        min_amount = float(min_amount_input) if min_amount_input.replace('.', '', 1).isdigit() else None
        max_amount = float(max_amount_input) if max_amount_input.replace('.', '', 1).isdigit() else None
    else:
        region, min_amount, max_amount = None, None, None

    # Call the filter function
    filtered_transactions, invalid_count, summary = file_handler.validate_and_filter(parsed_sales_data, region, min_amount, max_amount)

    print("\nOutput:")
    print("(")
    print(f"    {filtered_transactions},")
    print(f"    {invalid_count},  # count of invalid transactions")
    print("    {")
    print(f"        'total_input': {summary['total_input']},")
    print(f"        'invalid': {summary['invalid']},")
    print(f"        'filtered_by_region': {summary['filtered_by_region']},")
    print(f"        'filtered_by_amount': {summary['filtered_by_amount']},")
    print(f"        'final_count': {summary['final_count']}")
    print("    }")
    print(")\n")

    logger.info("Output:")
    logger.info("(")
    logger.info(f"    {filtered_transactions},")
    logger.info(f"    {invalid_count},  # count of invalid transactions")
    logger.info("    {")
    logger.info(f"        'total_input': {summary['total_input']},")
    logger.info(f"        'invalid': {summary['invalid']},")
    logger.info(f"        'filtered_by_region': {summary['filtered_by_region']},")
    logger.info(f"        'filtered_by_amount': {summary['filtered_by_amount']},")
    logger.info(f"        'final_count': {summary['final_count']}")
    logger.info("    }")
    logger.info(")\n")

    logger.info(f"-"*75)
    logger.info("[4/10] Validating transactions...")
    logger.info(f"-"*75)
    print("[4/10] Validating transactions...")

    logger.info(f"✓ Valid: {len(filtered_transactions)} | Invalid: {invalid_count}\n")
    print(f"✓ Valid: {len(filtered_transactions)} | Invalid: {invalid_count}\n")

    logger.info(f"-"*75)
    logger.info("Part 2: Data Processing (Lists, Dictionaries & Functions)")
    logger.info(f"-"*75) 

    logger.info("[5/10] Analyzing sales data...\n")
    logger.info(f"-"*75)
    print("[5/10] Analyzing sales data...")    

    from utils.data_processor import DataProcessor

    data_processor = DataProcessor(logger = logger)

    logger.info(" Task 2.1 : Sales Summary Calculator...")
    logger.info(f"-"*75)

    # 1. Total Revenue
    logger.info("Calculating total revenue...")

    total_revenue = data_processor.calculate_total_revenue(parsed_sales_data)

    logger.info("Total Revenue: ₹%s\n", total_revenue)
    print(f"\nTotal Revenue: ₹{total_revenue}")    
    
    # 2. Region-wise Sales Analysis

    region_sales = data_processor.region_wise_sales(parsed_sales_data)

    logger.info("Region-wise Sales Analysis: \n")
    print("\nRegion-wise Sales Analysis:\n")

    logger.info(f"Region-wise Sales: {region_sales}\n")
    print(f"Region-wise Sales: {region_sales}")

    for region, stats in region_sales.items():
        logger.info("Region: %s: Total Sales = ₹%s, Transactions = %s, Percentage = %s %%", region, stats['total_sales'], stats['transaction_count'], stats['percentage']) 
    logger.info("\n") 

    # 3. Top Selling Products
    logger.info("Top Selling Products: \n")
    print("\nTop Selling Products:\n")
    
    top_products = data_processor.top_selling_products(parsed_sales_data, n=5)    

    logger.info(f"Top {len(top_products)} Selling Products Identified: {top_products}\n")
    print(f"Top {len(top_products)} Selling Products Identified: {top_products}\n")

    for product, qty, revenue in top_products:
        logger.info(f"{product}: Quantity Sold = {qty}, Revenue = ₹{revenue}")
    logger.info("\n")

    # 4. Customer Purchase Analysis

    logger.info("Customer Purchase Analysis: \n")
    print("Customer Purchase Analysis:\n")

    customer_stats = data_processor.customer_analysis(parsed_sales_data)
    
    logger.info("Customer Purchase Analysis: %s\n", customer_stats)
    print(f"Customer Purchase Analysis: {customer_stats}\n")    

    logger.info(f"-"*75)
    logger.info(" Task 2.2 : Date-based Analysis...")
    logger.info(f"-"*75)

    # 1. Daily Sales Trend
    logger.info("Daily Sales Trend:\n")
    print("Daily Sales Trend:\n")

    daily_sales_trend = data_processor.daily_sales_trend(parsed_sales_data)
    
    logger.info("Daily Sales Trend: %s\n", daily_sales_trend)
    print(f"Daily Sales Trend: {daily_sales_trend}\n")    
    
    # 2. Peak Sales Day
    logger.info("Peak Sales Day:\n")
    print("Peak Sales Day:\n")

    peak_data = data_processor.find_peak_sales_day(parsed_sales_data)
    
    logger.info("Peak Sales Day: %s", peak_data)
    logger.info("Peak Sales Day: %s with Revenue: ₹%s and transaction_count: %s", peak_data[0], peak_data[1],  peak_data[2])
    print(f"Peak Sales Day: {peak_data}\n")
    
    logger.info(f"-"*75)
    logger.info(" Task 2.3 : Product Performance...")
    logger.info(f"-"*75)

    # 1. Low Performing Products
    logger.info("Low Performing Products (Sold less than 10 units):\n")
    print("Low Performing Products (Sold less than 10 units):\n")       
    
    low_performing_products = data_processor.low_performing_products(parsed_sales_data, threshold=10)

    logger.info("Low Performing Products: %s\n", low_performing_products)
    print("Low Performing Products: %s\n" % low_performing_products)    

    logger.info(f"✓ Analysis complete\n")
    print(f"✓ Analysis complete\n")

    logger.info("[6/10] Fetching product data from API...\n")
    print("[6/10] Fetching product data from API...\n")    

    # Task 3.1: Fetch Product Details from API and Create Product Mapping
    from utils.api_handler import ApiHandler

    api_handler = ApiHandler(logger=logger)

    # a) Fetch All Products
    logger.info("Fetching products from API...")

    all_products = api_handler.fetch_all_products()
    
    if not all_products:
        print("No products fetched from API.\n")
        logger.warning("No products fetched from API.\n")
    else:        
        logger.info("Products Dictionary fetched from API: %s\n", all_products)
        logger.info(f"✓ Fetched {len(all_products)} products from API\n")
        print(f"Products Dictionary fetched from API: {all_products}\n")
    
    # b) Create Product Mapping
    logger.info("Creating product mapping from fetched products...")

    product_mapping = api_handler.create_product_mapping(all_products)

    logger.info("Product Mapping: %s\n", product_mapping)
    logger.info(f"✓ Created {len(product_mapping)} product mapping from fetched products\n")
    print ("Product Mapping created: %s\n" % product_mapping)

    logger.info(f"✓ Fetched {len(all_products)} products from API and created product mapping\n")
    print(f"✓ Fetched {len(all_products)} products\n")

    


# Run main function
if __name__ == "__main__":
    main()