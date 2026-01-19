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
    
    