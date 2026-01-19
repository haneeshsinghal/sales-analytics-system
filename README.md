# Sales Analytics System
---

**Student Name:** Haneesh Singhal  
**Student ID:** BA_25071841  
**Email:** haneesh_singhal@yahoo.com  
**Date:** 19-Jan-2026  

---

A modular Python-based system for cleaning, analyzing, enriching, and reporting on e-commerce sales data. It demonstrates file handling, data processing, API integration, and automated report generation.

---

## 📁 Project Structure

```
sales-analytics-system/
├── README.md
├── main.py
├── utils/
│   ├── file_handler.py
│   ├── data_processor.py
│   └── api_handler.py
├── data/
│   └── sales_data.txt
├── output/
│   └── sales_report.txt
├── requirements.txt
```
---

## Key Features

### 1. File Handling & Preprocessing (`utils/file_handler.py`)
- **read_sales_data(filename):** Reads sales data from a pipe-delimited file, handles encoding issues, validates records.
- **parse_transactions(raw_lines):** Parses and cleans raw lines into dictionaries, handling commas, data types, and malformed records.
- **validate_and_filter(transactions, region=None, min_amount=None, max_amount=None):** Validates transactions and applies optional region/amount filters.

### 2. Data Processing (`utils/data_processor.py`)
- **calculate_total_revenue(transactions):** Computes total revenue.
- **region_wise_sales(transactions):** Analyzes sales by region.
- **top_selling_products(transactions, n=5):** Finds top N products by quantity sold.
- **customer_analysis(transactions):** Analyzes customer purchase patterns.
- **daily_sales_trend(transactions):** Analyzes sales trends by date.
- **find_peak_sales_day(transactions):** Finds the day with highest revenue.
- **low_performing_products(transactions, threshold=10):** Identifies products with low sales.

### 3. API Integration (`utils/api_handler.py`)
- **fetch_all_products():** Fetches 109 product data from DummyJSON API to show realistic scenario for enriched data.
- **create_product_mapping(api_products):** Maps product IDs to API info.
- **enrich_sales_data(transactions, product_mapping):** Enriches sales data with API info and saves to `data/enriched_sales_data.txt`.
- **save_enriched_data(enriched_transactions, filename):** Saves enriched data to file.

### 4. Report Generation
- **generate_sales_report(transactions, enriched_transactions, output_file):** Generates a comprehensive report in `output/sales_report.txt` with summaries, tables, trends, and enrichment stats.

### 5. Main Application (`main.py`)
- Orchestrates the workflow: reading, cleaning, filtering, analyzing, enriching, saving, and reporting.
- Handles user interaction for filtering and displays progress/status messages.

---

## 🛠 Technologies Used

- **Python 3.x**
- **Standard Libraries:** os, sys, datetime, etc.
- **Third-party:** requests (for API calls)
- **File formats:** Pipe-delimited text files

---

## 📦 Setup & Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/sales-analytics-system.git
   cd sales-analytics-system
   ```

2. **Install dependencies:** Will install automatically during execution.
   ```bash
   pip install -r requirements.txt
   ```
   
3. **Input Data file location:**
   ```
   Place sales_data.txt in the data/ folder
   ```
---
## How to Run
```
python main.py
```
The program will:

- Install Dependencies
- Read and clean sales data
- Offer filtering options (by region, amount)
- Analyze and enrich data with API info
- Save enriched data to data/enriched_sales_data.txt
- Generate a detailed report at output/sales_report.txt
- Display progress and summary in the console

---
## Output Files
- **data/enriched_sales_data.txt:** Cleaned and API-enriched sales data.
- **output/sales_report.txt:** Comprehensive sales analytics report.

---
## Example Console Output
---

[1/10] Reading sales data... ✓ Successfully read "X" transactions
[2/10] Parsing and cleaning data... ✓ Parsed "X" records
[3/10] Filter Options Available: Regions: "North", South, East, West
Amount Range: ₹X - ₹Y
Do you want to filter data? (y/n): n
[4/10] Validating transactions... ✓ Valid: "X"  Invalid: "Y"
[5/10] Analyzing sales data... ✓ Analysis complete
[6/10] Fetching product data from API... ✓ Fetched "X" products
[7/10] Enriching sales data... ✓ Enriched X/Y transactions (Z%)
[8/10] Saving enriched data... ✓ Saved to: data/enriched_sales_data.txt
[9/10] Generating report... ✓ Report saved to: output/sales_report.txt
[10/10] Process Complete!