# ---------------------------------
# Data Processor Module
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
        # Fetch 109 products from the API with retry logic to show the real scenario for enrichment
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
        # Create a mapping from product ID to product details
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

