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
    """
    Find the project root directory by looking for specific marker files or directories.
    
    parameters:
     - param preferred_dirs: List of directory names to look for as markers.
     - param max_up: Maximum number of parent directories to traverse up.

    returns: Path object of the project root directory.
    """
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
    """
    Ensure that the specified directories exist.

    parameters:
     - param dirs: List of directory paths to ensure existence.

    returns: None
    """
    for directory in dirs:
        Path(directory).mkdir(parents=True, exist_ok=True)

# Install required packages
def install_package(package_file):
    """
    Install a package using pip dynamically.

    parameters:
     - param package_file: Path to the requirements file.

    returns: None
    """
    if os.path.exists(package_file):
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', "-r", str(package_file)])

import logging

# ----------- Setup logging -----------
# Configuring logging to display messages to the console

# Setup logging
def setup_logging(log_file_name, logs_level = logging.INFO):
    """
    Setup logging configuration.

    parameters:
     - param log_file_name: Name of the log file.
     - param logs_level: Logging level (default: logging.INFO).

    returns: None
    """
    logging.basicConfig(    
        filename=str(log_file_name),
        level=logs_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        encoding='utf-8'
    )

# --------------------------
# Report Generation Function
# --------------------------
def generate_sales_report(sales_data, enriched_data, non_enriched_data, report_file_path):
    """
    Generate a comprehensive sales report.

    parameters:
     - param sales_data: List of all sales transactions.
     - param enriched_data: List of enriched sales transactions.
     - param non_enriched_data: List of non-enriched sales transactions.
     - param report_file_path: Path to save the generated report.

    returns: None
    """
    # Inline function to format money values
    def fmt_money_val(value):
        return f"₹{value:,.2f}"
    
    # Inline function to calculate percentage
    def percentage(value, total):
        return "{:.2f}%".format((value / total) * 100) if total > 0 else "0.00%"

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_records = len(sales_data)

    # Overall Sales Summary
    revenues = []

    for txn in sales_data:
        revenues.append(float(txn.get('Quantity', 0)) * float(txn.get('Price', 0.0)))
    total_revenue = sum(revenues)  
    total_transactions = len(sales_data)

    average_order_value = total_revenue / total_transactions if total_transactions > 0 else 0.0
    dates = sorted(txn['Date'] for txn in sales_data)
    date_range = f"{dates[0]} to {dates[-1]}" if dates else "N/A"


    # REGION-WISE PERFORMANCE
    region_sales = defaultdict(float)
    region_counts = Counter()

    for txn in sales_data:
        region = txn.get('Region', 'Unknown')
        region_sales[region] += float(txn.get('Quantity', 0)) * float(txn.get('Price', 0.0))
        region_counts[region] += 1

    region_total = sum(region_sales.values())
    # Sort Region by sales descending
    sorted_region_sales = sorted(region_sales.items(), key=lambda x: x[1], reverse=True)

    # TOP 5 PRODUCTS
    product_sales = defaultdict(dict)
    for txn in sales_data:
        product = txn.get('ProductName', 'Unknown')
        if product not in product_sales:
            product_sales[product] = {'quantity': 0, 'revenue': 0.0}

        quantity = int(txn.get('Quantity', 0))
        revenue = float(txn.get('Quantity', 0)) * float(txn.get('Price', 0.0))
        
        product_sales[product]['quantity'] += quantity
        product_sales[product]['revenue'] += revenue

    # Sort products by revenue descending
    top_products = sorted(product_sales.items(), key=lambda x: x[1]['revenue'], reverse=True)[:5]


    # TOP 5 CUSTOMERS
    customer_sales = defaultdict(dict)
    for txn in sales_data:
        customer = txn.get('CustomerID', 'Unknown')
        if customer not in customer_sales:
            customer_sales[customer] = {'spent': 0.0, 'orders': 0}

        quantity = int(txn.get('Quantity', 0))
        revenue = float(txn.get('Quantity', 0)) * float(txn.get('Price', 0.0))       
        
        customer_sales[customer]['spent'] += revenue
        customer_sales[customer]['orders'] += 1
    # Sort customers by amount spent descending
    top_customers = sorted(customer_sales.items(), key=lambda x: x[1]['spent'], reverse=True)[:5]

    # DAILY SALES TREND

    daily_sales = defaultdict(dict)
    for txn in sales_data:
        date = txn.get('Date', 'Unknown')
        if date not in daily_sales:
            daily_sales[date] = {'revenue': 0.0, 'transactions': 0, 'customers': set()}

        revenue = float(txn.get('Quantity', 0)) * float(txn.get('Price', 0.0))       
        
        daily_sales[date]['revenue'] += revenue
        daily_sales[date]['transactions'] += 1
        daily_sales[date]['customers'].add(txn.get('CustomerID', 'Unknown'))

    daily_sales_rows = list(daily_sales.items())
    # Sort by Date
    daily_sales_rows.sort(key=lambda x: x[0])

    # PRODUCT PERFORMANCE ANALYSIS
    # BEST SELLING DAY
    best_selling_day = max(daily_sales.items(), key=lambda x: x[1]['revenue'])[0 ] if daily_sales else "N/A"
    low_performing_products = [product for product, stats in product_sales.items() if stats['quantity']  == min(x['quantity'] for x in product_sales.values())]
    average_txn_region = {region: region_sales[region] / region_counts[region] if region_counts[region] > 0 else 0.0 for region in region_sales}
    
    # API ENRICHMENT SUMMARY
    enriched_sales_count = sum(1 for txn in enriched_data if txn.get('API_Match') == 'True')
    success_rate = (enriched_sales_count / len(enriched_data)) * 100 if enriched_data else 0
    not_enriched = [txn['ProductID'] for txn in non_enriched_data if txn.get('API_Match') != 'True']

    # Write Report to File
    with open(report_file_path, 'w', encoding='utf-8') as report_file:
        report_file.write("="*60 + "\n")
        report_file.write(f"                 SALES ANALYTICS REPORT\n")
        report_file.write(f"                 Generated: {now}\n")
        report_file.write(f"                 Records Processed: {total_records}\n")
        report_file.write("="*60 + "\n\n")

        # Overall Summary
        report_file.write(f"OVERALL SUMMARY\n")
        report_file.write("-"*60 + "\n")
        report_file.write(f"Total Revenue:          {fmt_money_val(total_revenue)}\n")
        report_file.write(f"Total Transactions:     {total_transactions}\n")
        report_file.write(f"Average Order Value:    {fmt_money_val(average_order_value)}\n")
        report_file.write(f"Date Range:             {date_range}\n")
        report_file.write("-"*60 + "\n")

        # Region-wise Performance
        report_file.write(f"REGION-WISE PERFORMANCE\n")
        report_file.write("-"*60 + "\n")
        report_file.write(f"{'Region':<15}{'Sales':<20}{'% of Total':<15}{'Transactions':<15}\n")
        for region, sales in sorted_region_sales:
            report_file.write(f"{region:<15}{fmt_money_val(sales):<20}{percentage(sales, region_total):<15}{region_counts[region]:<15}\n")
        report_file.write("-"*60 + "\n")

        # Top 5 Products
        report_file.write(f"TOP 5 PRODUCTS\n")
        report_file.write("-"*60 + "\n")
        report_file.write(f"{'Rank':<10}{'Product Name':<20}{'Qty Sold':<15}{'Revenue':<15}\n")
        for idx, (pname, pdata) in enumerate(top_products, 1):
            report_file.write(f"{idx:<10}{pname:<20}{pdata['quantity']:<15}{fmt_money_val(pdata['revenue']):<15}\n")
        report_file.write("-"*60 + "\n")

        # Top 5 Customers
        report_file.write(f"TOP 5 CUSTOMERS\n")
        report_file.write("-"*60 + "\n")
        report_file.write(f"{'Rank':<10}{'Customer ID':<15}{'Total Spent':<15}{'Order Count':<15}\n")
        for idx, (cid, cdata) in enumerate(top_customers, 1):
            report_file.write(f"{idx:<10}{cid:<15}{fmt_money_val(cdata['spent']):<15}{cdata['orders']:<15}\n")
        report_file.write("-"*60 + "\n")

        # Daily Sales Trend
        report_file.write(f"DAILY SALES TREND\n")
        report_file.write("-"*60 + "\n")
        report_file.write(f"{'Date':<15}{'Revenue':<20}{'Transactions':<15}{'Unique Customers':<15}\n")
        for date_idx, val_idx in daily_sales_rows:
            report_file.write(f"{date_idx:<15}{fmt_money_val(val_idx['revenue']):<20}{val_idx['transactions']:<15}{len(val_idx['customers']):<15}\n")
        report_file.write("-"*60 + "\n")

        # Product Performance Analysis
        report_file.write(f"PRODUCT PERFORMANCE ANALYSIS\n")
        report_file.write("-"*60 + "\n")
        report_file.write(f"Best Selling Day:        {best_selling_day}\n")
        report_file.write(f"Low Performing Products: {', '.join(low_performing_products) if low_performing_products else 'None'}\n\n")
        report_file.write("Average Transaction Value per Region:\n")
        for r_idx, val in average_txn_region.items():
            report_file.write(f"  {r_idx:<7}:   {fmt_money_val(val)}\n")
        report_file.write("-"*60 + "\n")

        # API Enrichment Summary
        report_file.write(f"API ENRICHMENT SUMMARY\n")
        report_file.write("-"*60 + "\n")
        report_file.write(f"Total Products Enriched: {enriched_sales_count}\n")
        report_file.write(f"Success Rate: {success_rate:.2f}%\n")
        report_file.write(f"Products Not Enriched: {', '.join(not_enriched) if not_enriched else 'None'}\n")
        report_file.write("-"*60)

# ------------------
# Main function
# ------------------

def main():          
    """
    Main function to run the sales analytics system.
    """
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
    
    # Task 3.2: Enrich Sales Data
    logger.info("[7/10] Enriching sales data...\n")
    print("[7/10] Enriching sales data...\n")

    enriched_sales_data, non_enriched_sales_data, total_enriched_count, total_non_enriched_count = api_handler.enrich_sales_data(parsed_sales_data, product_mapping, str(enriched_sales_data_path))
    percentage = (total_enriched_count / (total_enriched_count + total_non_enriched_count)) * 100 if (total_enriched_count + total_non_enriched_count) > 0 else 0

    for txn in enriched_sales_data:
        print(txn)
        logger.info(txn)

    for txn in non_enriched_sales_data:
        print(txn)
        logger.info(txn)

    logger.info("\n")
    logger.info(f"✓ Enriched {total_enriched_count} / {total_enriched_count + total_non_enriched_count} transactions {percentage:.2f}%\n")
    print(f"\n✓ Enriched {total_enriched_count} / {total_enriched_count + total_non_enriched_count} transactions {percentage:.2f}%\n")
    
    logger.info("[8/10] Saving enriched data...\n")
    print("[8/10] Saving enriched data...\n")

    short_path = os.path.join(os.path.basename(os.path.dirname(enriched_sales_data_path)), os.path.basename(enriched_sales_data_path))

    logger.info("✓ Saved to: %s\n", short_path)
    print(f"✓ Saved to: {short_path}\n")

    # Task 4: Generate Sales Report File

    logger.info("[9/10] Generating report...\n")
    print("[9/10] Generating report...\n")

    generate_sales_report(parsed_sales_data, enriched_sales_data, non_enriched_sales_data, str(report_file_path))

    short_path = os.path.join(os.path.basename(os.path.dirname(report_file_path)), os.path.basename(report_file_path))

    logger.info(f"✓ Report saved to: {short_path}\n")
    print(f"✓ Report saved to: {short_path}\n")

    logger.info("[10/10] Process Complete!\n")
    print("[10/10] Process Complete!\n")
    
    logger.info(f"="*60)
    print(f"="*60)


# Run main function
if __name__ == "__main__":
    main()