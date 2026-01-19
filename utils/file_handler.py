import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime, date
from venv import logger

# ---------------------------------
# Sales Data File Handler Class
# ---------------------------------
class SalesDataFileHandler:
    def __init__(self, logger = None):
        self.logger = logger or logging.getLogger(__name__)

    # ---------------------------------
    # Read Sales data from text file
    # ---------------------------------

    # Read sales data from file
    def read_sales_data(self, filename: str) -> Tuple[List[str], Optional[str]]:
        
        sales_data_without_header: List[str] = []

        if not filename:
            self.logger.warning("No filename provided to read sales data.")
            return [], None
    
        file_name = Path(filename)
        if not file_name.exists():
            self.logger.warning("File not found: %s", filename)
            return [], None
        
        # Try different encodings to read the file
        encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']

        try:
            encoding_used = None
            for encoding in encodings_to_try:
                with open(file_name, 'r', encoding=encoding, newline='\n') as file:
                    
                    # Split into lines and clean
                    sales_data_without_header: List[str] = []

                    for lines in file:
                        line = lines.strip()
                        if not line:
                            continue
                        if line.startswith('TransactionID|'):
                            continue  # Skip header line
                        sales_data_without_header.append(line)

                encoding_used = encoding
                break

            # if none of the encoding workeds
            if encoding_used is None:
                self.logger.warning("Could not read '%s' with any known encoding. Detected = %s, tried %s", filename, None, encodings_to_try)
                return [], None

            # Enforce 50 - 100 lines (inclusive)

            min_transaction_lines = 50
            max_transaction_lines = 100
            count = len(sales_data_without_header)

            if count < min_transaction_lines:
                self.logger.warning("File '%s' has too few data lines: %d. Minimum required is %d.", filename, count, min_transaction_lines)
                return [], encoding_used

            if count > max_transaction_lines:
                self.logger.warning("File '%s' has too many data lines: %d. Maximum allowed is %d.", filename, count, max_transaction_lines)
                return [], encoding_used

        except UnicodeDecodeError as ude:
            self.logger.error("Encoding error while reading file '%s': %s", filename, str(ude))
        except ValueError as ve:
            self.logger.error("Value error while reading file '%s': %s", filename, str(ve))
        except FileNotFoundError as fnfe:
            self.logger.error("File not found: %s", filename)
        except Exception as e:
            self.logger.error("Unexpected error while reading file '%s': %s", filename, str(e))

        return sales_data_without_header, encoding_used
    # ---------------------------------

    # ---------------------------------
    # Parse and Clean Sales Transactions
    # ---------------------------------

    # Parse and clean sales transactions
    def parse_transactions(self, raw_lines: List[str]) -> List[Dict]:
        
        parsed_transactions: List[Dict] = []

        required_num_fields = 8
        total_records_parsed = 0
        invalid_records_removed = 0
        valid_records_after_cleaning = 0
        dates_list: List[date] = []

        if raw_lines is None or len(raw_lines) == 0:
            self.logger.info("No raw lines provided for parsing.")
            self.logger.info("Total rows parsed: 0")
            self.logger.info("Invalid records removed: 0")
            self.logger.info("Valid records after cleaning: 0")
            self.logger.info("Date range: N/A")
            return []

        required_fields = [
            'TransactionID', 
            'Date', 
            'ProductID', 
            'ProductName',
            'Quantity', 
            'Price', 
            'CustomerID', 
            'Region'
        ]

        for idx, line in enumerate(raw_lines, start=1):
            try:
                if line is None or line.strip() == "":
                    self.logger.info("Skipping empty line at index %d.", idx)
                    continue        

                raw_transaction_line = line.strip()

                total_records_parsed += 1

                fields = raw_transaction_line.split('|')

                if len(fields) != required_num_fields:
                    self.logger.info("Skipping line %d due to incorrect number of fields: expected %d, got %d.", idx, required_num_fields, len(fields))
                    invalid_records_removed += 1
                    continue

                fields = [field.strip() for field in fields]
                transaction_dict = dict(zip(required_fields, fields))                

                try:

                    transaction_date = datetime.strptime(transaction_dict['Date'], '%Y-%m-%d').date()
                    dates_list.append(transaction_date)
                except ValueError as ve:
                    self.logger.error("Skipping line %d due to invalid date format: %s", idx, str(ve))
                    invalid_records_removed += 1
                    continue

                product_name = transaction_dict['ProductName'].replace(',', ' ').strip()
                while '  ' in product_name:
                    product_name = product_name.replace('  ', ' ')

                quantity_str = (transaction_dict['Quantity'] or "").replace(',', '').strip()                
                try:
                    quantity = int(quantity_str)
                    if not str(quantity).isdigit() or quantity <= 0:
                        self.logger.warning("Skipping line %d due to invalid quantity values: %s", idx, quantity_str)
                        invalid_records_removed += 1
                        continue 
                                        
                except (ValueError, TypeError) as ve:
                    self.logger.error("Skipping line %d due to invalid quantity format: %s", idx, quantity_str)
                    invalid_records_removed += 1
                    continue          
                
                price_str = (transaction_dict['Price'] or "").replace(',', '').strip()
                try:
                    price = float(price_str)
                    if not str(price).replace('.', '', 1).isdigit() or price <= 0:
                        invalid_records_removed += 1
                        self.logger.warning("Skipping line %d due to invalid price values: %s", idx, price_str)
                        continue
                     
                except ValueError as ve:
                    self.logger.error("Skipping line %d due to invalid Price values: %s", idx, price_str)
                    invalid_records_removed += 1
                    continue             
                 
                if not transaction_dict['TransactionID'] or not transaction_dict['TransactionID'].startswith('T'):
                    self.logger.warning("Skipping line %d due to missing or invalid TransactionID: %s", idx, transaction_dict['TransactionID'])
                    invalid_records_removed += 1
                    continue                

                if not transaction_dict['CustomerID'] or not transaction_dict['CustomerID'].strip() or not transaction_dict['Region'] or not transaction_dict['Region'].strip():
                    self.logger.warning("Skipping line %d due to missing CustomerID or Region (CUSTOMER ID: '%s', REGION: '%s').", idx, transaction_dict['CustomerID'], transaction_dict['Region'])
                    invalid_records_removed += 1
                    continue
                    
                # Validate TransactionID format
                
                Clean_transaction_record = {
                    'TransactionID': transaction_dict['TransactionID'],
                    'Date': transaction_date.strftime('%Y-%m-%d'),  
                    'ProductID': transaction_dict['ProductID'],
                    'ProductName': product_name,
                    'Quantity': quantity,
                    'Price': price,
                    'CustomerID': transaction_dict['CustomerID'],
                    'Region': transaction_dict['Region']
                }
                
                parsed_transactions.append(Clean_transaction_record)
                valid_records_after_cleaning += 1

            except Exception as e:
                self.logger.error("Skipping line %d due to unexpected error: %s", idx, str(e))
                invalid_records_removed += 1
                continue

            # Build Date Range from dates_list
        if dates_list:
            min_date = min(dates_list)
            max_date = max(dates_list)
            date_range_str = f"{min_date.strftime('%B %Y')} to {max_date.strftime('%B %Y')}"
        else:
            date_range_str = "N/A"

        # Summary Report
        self.logger.info(f"-"*75)
        self.logger.info("Total rows parsed:            %d", total_records_parsed)
        self.logger.info("Invalid records removed:      %d", invalid_records_removed)
        self.logger.info("Valid records after cleaning: %d", valid_records_after_cleaning)
        self.logger.info("Date range:                   %s", date_range_str)
        self.logger.info(f"-"*75)
        self.logger.info("Finished parsing transactions. Total valid records: %d | Total invalid records: %d | Total records parsed: %d", valid_records_after_cleaning, invalid_records_removed, total_records_parsed)
        
        print("Total rows parsed:            %d" % total_records_parsed)
        print("Invalid records removed:      %d" % invalid_records_removed)
        print("Valid records after cleaning: %d" % valid_records_after_cleaning)
        

        return parsed_transactions

    # ---------------------------------