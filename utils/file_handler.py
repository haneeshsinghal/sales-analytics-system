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

    # --------------------------------------
    # Validate and Filter Sales Transactions
    # --------------------------------------
    
    # Validate and filter sales transactions based on user criteria
    def validate_and_filter(self, transactions: List[Dict], region: str = "", min_amount: str = "", max_amount: str = "") -> Tuple[List[Dict], int, Dict]:

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

        valid_transactions: List[Dict] = []
        invalid_count = 0

        try:
            for txn in transactions:
                try:
                    # validate required fields
                    if not all (field in txn for field in required_fields):
                        self.logger.warning("Skipping transaction due to missing required fields: %s", txn)
                        invalid_count += 1
                        continue

                    #validate Quantity and Price
                    if not isinstance(txn['Quantity'], int) or txn['Quantity'] <= 0:
                        self.logger.warning("Skipping transaction due to invalid Quantity: %s", txn)
                        invalid_count += 1
                        continue

                    if not isinstance(txn['Price'], float) or txn['Price'] <= 0:
                        self.logger.warning("Skipping transaction due to invalid Price: %s", txn)
                        invalid_count += 1
                        continue

                    # Validate Ids
                    if not txn['TransactionID'].startswith('T'):
                        self.logger.warning("Skipping transaction due to invalid TransactionID: %s", txn)
                        invalid_count += 1
                        continue

                    if not txn['CustomerID'].startswith('C'):
                        self.logger.warning("Skipping transaction due to invalid CustomerID: %s", txn)
                        invalid_count += 1
                        continue

                    if not txn['ProductID'].startswith('P'):
                        self.logger.warning("Skipping transaction due to invalid ProductID: %s", txn)
                        invalid_count += 1
                        continue                    

                    valid_transactions.append(txn)

                

                except Exception as e:
                    self.logger.error("Skipping transaction due to unexpected error: %s | Transaction: %s", str(e), txn)
                    invalid_count += 1                

            # Summary before filtering
            summary = {
                'total_input': len(transactions),
                'invalid': invalid_count,
                'filtered_by_region': 0,
                'filtered_by_amount': 0,
                'final_count': 0
            }

            # If user chooses "n" for filters, skip filtering and return all valid transactions
            if (region == "" or region is None) and min_amount is None and max_amount is None:
                # No filters applied
                summary['filtered_by_region'] = 0
                summary['filtered_by_amount'] = 0
                summary['final_count'] = len(valid_transactions)

                self.logger.info("No filters applied. Showing all valid transactions.")
                return (valid_transactions, invalid_count, summary)
            else:
                self.logger.info("Applying filters - Region: %s, Min Amount: %s, Max Amount: %s", region, min_amount, max_amount)            
                # Apply Region filter
                region_filtered = valid_transactions.copy()

                if region:                
                    region_filtered = [txn for txn in valid_transactions if txn['Region'].lower() == region.lower()]
                    summary['filtered_by_region'] = len(region_filtered)
                    self.logger.info("Applied region filter '%s'. Transactions reduced from %d to %d.", region, len(valid_transactions), len(region_filtered))
                else:
                    summary['filtered_by_region'] = len(region_filtered)
                    self.logger.info("No region filter applied. Count: %d", len(region_filtered))

                remaining_transactions_without_region = [txn for txn in valid_transactions if txn not in region_filtered]

                # Amount filter
                amount_filtered = []
                for txn in remaining_transactions_without_region:  
                    amount = txn['Quantity'] * txn['Price']

                    #If both min_amount and max_amount are None → include all records
                    if min_amount is None and max_amount is None:
                        amount_filtered.append(txn)
                    # If only min_amount is provided
                    elif min_amount is not None and max_amount is None:
                        if amount >= min_amount:
                            amount_filtered.append(txn)
                    # If only max_amount is provided
                    elif min_amount is None and max_amount is not None:
                        if amount <= max_amount:
                            amount_filtered.append(txn)
                    # If both are provided → apply AND logic
                    else:
                        if amount >= min_amount and amount <= max_amount:
                            amount_filtered.append(txn)

                summary['filtered_by_amount'] = len(amount_filtered)
                
                remaining_transactions = [txn for txn in remaining_transactions_without_region if txn not in amount_filtered]

                summary['final_count'] = len(remaining_transactions)
                self.logger.info("Filtering process completed successfully.")

                # Return :
                return (remaining_transactions, invalid_count, summary)

        except Exception as e:
            logger.error(f"Unexpected error in validate_and_filter: {e}")
            return ([], invalid_count, {'error': str(e)})


# End of Class         