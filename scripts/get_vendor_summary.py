import pandas as pd
import sqlite3
import time
import logging
from ingestion_db import ingest_db


logging.basicConfig(
    filename = r"C:\Users\KR\Downloads\video\New folder\projects\data\data\logs\get_vendor_summary.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"  
)
logging.info("Check logging")
def create_vendor_summary(conn):
    ''' this function will merge the different tables to get the overall vendor summary and adding new columns in the resultant data '''
    import time
    start = time.time()
    vendor_sales_summary = pd.read_sql('''
    with freight_summary as(
    select VendorNumber, sum(Freight) as FreightCost
    from vendor_invoice
    group by VendorNumber
    ),
    
    purchase_summary as (
    
    select 
    p.VendorNumber,
    p.VendorName,
    p.Brand,
    p.Description,
    p.PurchasePrice,
    pp.Volume,
    pp.Price as Actual_Price,
    sum(p.Quantity) as Total_Purchase_Quantity,
    sum(p.Dollars) as Total_Purchase_Dollars
    from purchases as p
    join purchase_prices as pp on p.Brand = pp.Brand
    where p.PurchasePrice > 0
    group by p.VendorNumber,  p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),
    
    sales_summary as (
    
    select
    VendorNo,
    Brand,
    sum(SalesQuantity) as Total_Sales_Quantity,
    sum(SalesDollars) as Total_Sales_Dollars,
    sum(SalesPrice) as Total_Sales_Prices,
    sum(ExciseTax) as Total_Excise_Tax
    from sales
    group by VendorNo, Brand
    
    )
    
    select 
    ps.VendorNumber,  
    ps.VendorName, 
    ps.Brand, 
    ps.Description, 
    ps.PurchasePrice, 
    ps.Actual_Price, 
    ps.Volume,
    ps.Total_Purchase_Quantity,
    ps.Total_Purchase_Dollars,
    ss.Total_Sales_Quantity,
    ss.Total_Sales_Dollars,
    ss.Total_Sales_Prices,
    ss.Total_Excise_Tax,
    fs.FreightCost
    from purchase_summary as ps
    left join sales_summary as ss
        on ps.VendorNumber = ss.VendorNo
        and ps.Brand = ss.Brand
        left join freight_summary fs
        on ps.VendorNumber = fs.VendorNumber
    order by ps.Total_Purchase_Dollars desc
    ''',conn)
    end = time.time()
    print(f'Total minutes: {(end - start)/60}')
    return vendor_sales_summary

def clean_data(df):
    """this function will clean the data"""
    df['Volume'] = df['Volume'].astype('float')
    df.fillna(0,inplace = True)
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    
    # creating new columns
    vendor_sales_summary['Gross_Profit'] = vendor_sales_summary['Total_Sales_Dollars'] - vendor_sales_summary['Total_Purchase_Dollars']
    vendor_sales_summary['Profit_Margin'] = (vendor_sales_summary['Gross_Profit'] / vendor_sales_summary['Total_Sales_Dollars'] )*100
    vendor_sales_summary['Stock_Turnover'] = vendor_sales_summary['Total_Sales_Quantity'] / vendor_sales_summary['Total_Purchase_Quantity']
    vendor_sales_summary['Sales_Purchase_Ratio'] = vendor_sales_summary['Total_Sales_Dollars'] / vendor_sales_summary['Total_Purchase_Quantity']
    return df

if __name__ == "__main__":
    # creating data base connection
    conn = sqlite3.connect(r'inventory.db')
    
    # creating the vendor summary table (throgh CTE query)
    logging.info('Creating Vendor Summary Table-----')
    start = time.time() # to start counting time
    summary_df = create_vendor_summary(conn)
    end = time.time() 
    total_time = (end - start)/60 
    logging.info(f'time taken to create vendor summary table {total_time} minutes')
    logging.info(summary_df.head())

    # cleaning data
    logging.info('Cleaning data ------')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    # ingesting data
    logging.info('Ingesting data---')
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logging.info('Completed')
    
