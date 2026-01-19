# ---------------------------------
# Data Processor Module
# ---------------------------------
import logging

# DataProcessor  Class
class DataProcessor:
    def __init__(self, logger = None):
        self.logger = logger or logging.getLogger(__name__)

    # Task 2.1: Sales Summary Calculator
    # a} Calculates total revenue from sales data
    def calculate_total_revenue(self, sales_data):
        """
        Calculates total revenue from all transactions

        parameters:
        sales_data: list of dictionaries (each dictionary represents a sales record with 'Quantity' and 'Price' keys)

        Returns: float (total revenue)

        Expected Output: Single number representing sum of (Quantity * UnitPrice)
        Example: 1545000.50

        """
        total_revenue = 0.0
        for record in sales_data:
            try:
                revenue = int(record.get('Quantity', 0)) * float(record.get('Price', 0))
                total_revenue += revenue                
            except (ValueError, TypeError):
                self.logger.error(f"Invalid amount value in record: {record}")    
        return total_revenue
    
    # b} Analyzes sales data by region
    def region_wise_sales(self, sales_data):
        """
        Analyzes sales by region

        parameters:
        sales_data: list of dictionaries (each dictionary represents a sales record with 'Region', 'Quantity', and 'Price' keys)

        Returns: dictionary with region statistics

        Expected Output Format:
        {
            'North': {
                'total_sales': 450000.0,
                'transaction_count': 15,
                'percentage': 29.13
            },
            'South': {...},
            ...
        }

        """
        region_sales = {}
        total_revenue = self.calculate_total_revenue(sales_data)

        for record in sales_data:
            region = record['Region']
            try:
                amount = int(record['Quantity']) * float(record['Price'])
            except (ValueError, TypeError):
                self.logger.error(f"Invalid amount value in record: {record}")
                amount = 0.0

            if region not in region_sales:
                region_sales[region] = {'total_sales': 0.0, 'transaction_count': 0}
            
            region_sales[region]['total_sales'] += amount
            region_sales[region]['transaction_count'] += 1

        # Calculate percentage contribution and sort regions by total sales
        for region in region_sales:
            region_sales[region]['percentage'] = round((region_sales[region]['total_sales'] / total_revenue) * 100, 2) if total_revenue > 0 else 0.0

        sorted_region_sales = dict(sorted(region_sales.items(), key=lambda item: item[1]['total_sales'], reverse=True))

        return sorted_region_sales
    
    # c} Identifies top N selling products based on quantity sold
    def top_selling_products(self, sales_data, n=5):
        """
        Finds top n products by total quantity sold

        parameters:
        sales_data: list of dictionaries (each dictionary represents a sales record with 'ProductName',
        
        Returns: list of tuples

        Expected Output Format:
        [
            ('Laptop', 45, 2250000.0),  # (ProductName, TotalQuantity, TotalRevenue)
            ('Mouse', 38, 19000.0),
            ...
        ]

        """
        product_sales = {}

        for record in sales_data:
            product = record['ProductName']
            try:
                quantity = int(record['Quantity'])
                revenue = quantity * float(record['Price'])
            except (ValueError, TypeError):
                self.logger.error(f"Invalid amount value in record: {record}")
                quantity = 0
                revenue = 0.0

            if product not in product_sales:
                product_sales[product] = {'total_quantity': 0, 'total_revenue': 0.0}
            
            product_sales[product]['total_quantity'] += quantity
            product_sales[product]['total_revenue'] += revenue

        # Sort products by quantity sold
        sorted_products = sorted(product_sales.items(), key=lambda item: item[1]['total_quantity'], reverse=True)
        
        top_n_products = [(product, stats['total_quantity'], round(stats['total_revenue'], 2)) for product, stats in sorted_products[:n]]
        
        return top_n_products
    
    # d} Analyzes customer purchase behavior
    def customer_analysis(self, sales_data):
        """
        Analyzes customer purchase patterns

        Returns: dictionary of customer statistics

        Expected Output Format:
        {
            'C001': {
                'total_spent': 95000.0,
                'purchase_count': 3,
                'avg_order_value': 31666.67,
                'products_bought': ['Laptop', 'Mouse', 'Keyboard']
            },
            'C002': {...},
            ...
        }

        """
        customer_stats = {}

        for record in sales_data:
            customer_id = record['CustomerID']
            product_name = record['ProductName']
            try:
                revenue = int(record['Quantity']) * float(record['Price'])
            except (ValueError, TypeError):
                self.logger.error(f"Invalid amount value in record: {record}")
                revenue = 0.0

            if customer_id not in customer_stats:
                customer_stats[customer_id] = {'total_spent': 0.0, 'purchase_count': 0, 'products': set()}
            
            customer_stats[customer_id]['total_spent'] += revenue
            customer_stats[customer_id]['purchase_count'] += 1
            customer_stats[customer_id]['products'].add(product_name)

        # Calculate average order value
        for customer_id in customer_stats:
            stats = customer_stats[customer_id]
            stats['avg_order_value'] = round(stats['total_spent'] / stats['purchase_count'], 2) if stats['purchase_count'] > 0 else 0.0
            stats['products_bought'] = sorted(list(stats['products']))
            del stats['products']

        # Sorted by total spent descending
        sorted_customer_stats = dict(sorted(customer_stats.items(), key=lambda item: item[1]['total_spent'], reverse=True))
                
        return sorted_customer_stats
    
    # Task 2.2: Date-based Analysis
    # a) Daily Sales Trend
    def daily_sales_trend(self, sales_data):
        """
        Analyzes sales trends by date

        parameters:
        sales_data: list of dictionaries (each dictionary represents a sales record with 'Date', 'Quantity', 'Price', and 'CustomerID' keys)

        Returns: dictionary sorted by date

        Expected Output Format:
        {
            '2024-12-01': {
                'revenue': 125000.0,
                'transaction_count': 8,
                'unique_customers': 6
            },
            '2024-12-02': {...},
            ...
        }

        """
        daily_sales = {}

        for record in sales_data:
            date = record['Date']
            customer_id = record['CustomerID']
            try:
                amount = int(record['Quantity']) * float(record['Price'])
            except (ValueError, TypeError):
                self.logger.error(f"Invalid amount value in record: {record}")
                amount = 0.0

            if date not in daily_sales:
                daily_sales[date] = {
                    'total_revenue': 0.0, 
                    'transaction_count': 0,
                    'unique_customers': set()
                }
            
            daily_sales[date]['total_revenue'] += amount
            daily_sales[date]['transaction_count'] += 1
            daily_sales[date]['unique_customers'].add(customer_id)

        # Convert customers set to count and sort by date
        for date in daily_sales:
            daily_sales[date]['unique_customers'] = len(daily_sales[date]['unique_customers'])

        # Sort by date
        sorted_daily_sales = dict(sorted(daily_sales.items(), key=lambda item: item[0]))

        return sorted_daily_sales
    
    # b) Find Peak Sales Day

    def find_peak_sales_day(self, sales_data):
        """
        Identifies the date with highest revenue

        parameters:
        sales_data: list of dictionaries (each dictionary represents a sales record with 'Date', 'Quantity', and 'Price' keys)

        Returns: tuple (date, revenue, transaction_count)

        Expected Output Format:
        ('2024-12-15', 185000.0, 12)

        """
        daily_sales = {}

        for record in sales_data:
            date = record['Date']
            try:
                amount = int(record['Quantity']) * float(record['Price'])
            except (ValueError, TypeError):
                self.logger.error(f"Invalid amount value in record: {record}")
                amount = 0.0

            if date not in daily_sales:
                daily_sales[date] = {'revenue': 0.0, 'transaction_count': 0}
            
            daily_sales[date]['revenue'] += amount
            daily_sales[date]['transaction_count'] += 1

        # Find the peak sales day
        peak_day, peak_data = max(daily_sales.items(), key=lambda item: item[1]['revenue']) if daily_sales else (None, None)        
        
        return (peak_day, round(peak_data['revenue'], 2), peak_data['transaction_count'])
    
    # Task 2.3: Product Performance
    # a) Low Performing Products

    def low_performing_products(self, sales_data, threshold=10):
        """
        Identifies products with low sales

        Returns: list of tuples

        Expected Output Format:
        [
            ('Webcam', 4, 12000.0),  # (ProductName, TotalQuantity, TotalRevenue)
            ('Headphones', 7, 10500.0),
            ...
        ]

        """
        product_sales = {}

        for record in sales_data:
            product = record['ProductName']
            try:
                quantity = int(record['Quantity'])
                revenue = quantity * float(record['Price'])
            except (ValueError, TypeError):
                self.logger.error(f"Invalid amount value in record: {record}")
                quantity = 0
                revenue = 0.0

            if product not in product_sales:
                product_sales[product] = {'total_quantity': 0, 'total_revenue': 0.0}
            
            product_sales[product]['total_quantity'] += quantity
            product_sales[product]['total_revenue'] += revenue

        # Filter products below threshold
        low_performers = [(product, stats['total_quantity'], round(stats['total_revenue'], 2)) 
                          for product, stats in product_sales.items() if stats['total_quantity'] < threshold]

        # Sort by quantity sold ascending
        low_performing_products = sorted(low_performers, key=lambda x: x[1])

        return low_performing_products
    
# ------- End of DataProcessor Class ------- #