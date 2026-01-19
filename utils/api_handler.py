# ---------------------------------
# API Handler Module
# ---------------------------------
import logging
import time
import requests

# ---------------------------------
# API Handler Class
# ---------------------------------
class ApiHandler:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    # a) Fetch All Products    
    def fetch_all_products(self, max_retries=3, delay=2) -> list:

        """
        Fetches all products from DummyJSON API (use limit=109 to show realistic scenario for enrichment).

        Returns: list of product dictionaries

        Expected Output Format:
        [
            {
                'id': 1,
                'title': 'iPhone 9',
                'category': 'smartphones',
                'brand': 'Apple',
                'price': 549,
                'rating': 4.69
            },
            ...
        ]

        """
        BASE_URL = "https://dummyjson.com/products?limit=109" 
        attempt = 0

        # Retry logic for fetching data
        while attempt < max_retries:
            try:
                response = requests.get(BASE_URL, timeout=10)
                response.raise_for_status()
                data = response.json()
                products = data.get("products", [])

                normalized_products = []

                # Normalize product data
                for p in products:
                    normalized_products.append({
                        'id': p.get('id', None),
                        'title': p.get('title', None),
                        'category': p.get('category', None),
                        'brand': p.get('brand', None),
                        'price': p.get('price', None),
                        'rating': p.get('rating', None)
                    })
                return normalized_products
                
            except requests.exceptions.Timeout:
                self.logger.error(f"Attempt {attempt} timed out. Retrying ...")
            except requests.exceptions.ConnectionError:
                self.logger.error(f"Attempt {attempt} failed due to connection error. Retrying ...")
            except requests.exceptions.HTTPError as http_err:
                self.logger.error(f"HTTP error occurred: {http_err}.")
                break
            except Exception as e:
                self.logger.error(f"An unexpected error occurred: {e}.")
                break
            attempt += 1
            time.sleep(delay)
        self.logger.error("Failed to fetch products after multiple attempts.")
        return []
    

    # b) Create Product Mapping
    def create_product_mapping(self, products: list) -> dict:
        """
        Creates a mapping of product IDs to product info

        Parameters: api_products from fetch_all_products()

        Returns: dictionary mapping product IDs to info

        Expected Output Format:
        {
            1: {'title': 'iPhone 9', 'category': 'smartphones', 'brand': 'Apple', 'rating': 4.69},
            2: {'title': 'iPhone X', 'category': 'smartphones', 'brand': 'Apple', 'rating': 4.44},
            ...
        }
        """
        product_mapping = {}
        
        product_mapping = {
            product['id']: {
                'title': product['title'],
                'category': product['category'],
                'brand': product['brand'],
                'rating': product['rating']
            } for product in products if 'id' in product
        }
        return product_mapping
    
    # ---------------------------------
    # Task 3.2: Enrich Sales Data
    # ---------------------------------

    def enrich_sales_data(self, sales_data, product_mapping, enriched_data_path):
        
        """
        Enriches transaction data with API product information

        Parameters:
        - transactions: list of transaction dictionaries
        - product_mapping: dictionary from create_product_mapping()
        - enriched_data_path: file path to save enriched data

        Returns: list of enriched transaction dictionaries and enriched data path

        Expected Output Format (each transaction):
        {
            'TransactionID': 'T001',
            'Date': '2024-12-01',
            'ProductID': 'P101',
            'ProductName': 'Laptop',
            'Quantity': 2,
            'UnitPrice': 45000.0,
            'CustomerID': 'C001',
            'Region': 'North',
            # NEW FIELDS ADDED FROM API:
            'API_Category': 'laptops',
            'API_Brand': 'Apple',
            'API_Rating': 4.7,
            'API_Match': True  # True if enrichment successful, False otherwise
        }
        """        
        enriched_data = []
        non_enriched_data = []
        enriched_product_id = {}
        non_enriched_product_id = {}

        enriched_data_filepath = enriched_data_path
        
        # Enrich each sales record
        for record in sales_data:
            try:                
                product_id_str = ''.join(filter(str.isdigit, str(record.get('ProductID', ''))))
                numeric_product_id = int(product_id_str) if product_id_str else None
                api_product_id = product_mapping.get(numeric_product_id, {})
                
                # Add enriched fields to the record from API data if available
                if api_product_id:
                    record['API_Category'] = api_product_id.get('category', None)
                    record['API_Brand'] = api_product_id.get('brand', None)
                    record['API_Rating'] = api_product_id.get('rating', None)
                    record['API_Match'] = 'True'
                    enriched_product_id[numeric_product_id] = record.get('ProductID')
                    enriched_data.append(record)
                # Handle case where product ID is not found in API data
                else:
                    record['API_Category'] = None
                    record['API_Brand'] = None
                    record['API_Rating'] = None
                    record['API_Match'] = 'False'
                    non_enriched_product_id[numeric_product_id] = record.get('ProductID')
                    non_enriched_data.append(record)
            
            except Exception as e:
                self.logger.error(f"Error enriching transaction {record.get('TransactionID', 'Unknown')}: {e}")
                record['API_Category'] = None
                record['API_Brand'] = None
                record['API_Rating'] = None
                record['API_Match'] = 'False'
                non_enriched_data.append(record)

        self.save_enriched_data(enriched_data, enriched_data_filepath)
        total_enriched = len(enriched_data)
        total_non_enriched = len(non_enriched_data)
        
        self.logger.info(f"Total Enriched Product Id: {enriched_product_id}\n")
        self.logger.info(f"Total Non-Enriched Product Id: {non_enriched_product_id}\n")
        return enriched_data, non_enriched_data, total_enriched, total_non_enriched  


    # Save Enriched Data helper function

    def save_enriched_data(self, enriched_data: list, enriched_data_filename_path: str):
        """
        Saves enriched transactions back to file
        Parameters:
        - enriched_data: list of enriched transaction dictionaries
        - enriched_data_filename_path: file path to save enriched data

        Returns: None

        Expected File Format:
        TransactionID|Date|ProductID|ProductName|Quantity|UnitPrice|CustomerID|Region|API_Category|API_Brand|API_Rating|API_Match
        T001|2024-12-01|P101|Laptop|2|45000.0|C001|North|laptops|Apple|4.7|True
        ...

        """        
        if not enriched_data_filename_path:
            self.logger.warning("No filename provided to read sales data.")
            return [], None

        # Define header for the enriched data file
        header = [
        'TransactionID', 'Date', 'ProductID', 'ProductName', 'Quantity', 'Price',
        'CustomerID', 'Region', 'API_Category', 'API_Brand', 'API_Rating', 'API_Match'
        ]        
        
        try:    
            # Write enriched data to file
            with open(enriched_data_filename_path, 'w', encoding='utf-8') as f:
                f.write('|'.join(header) + '\n')
                for txn in enriched_data:
                    row = [ str(txn.get(col, '')) for col in header ]
                    f.write('|'.join(row) + '\n')

            self.logger.info(f"Enriched data saved to {enriched_data_filename_path}.\n")
        except Exception as e:
            self.logger.error(f"Error saving enriched data to {enriched_data_filename_path}: {e}")
                
# End of API Handler Class

