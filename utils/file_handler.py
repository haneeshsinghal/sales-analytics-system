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