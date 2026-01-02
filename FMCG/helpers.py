import logging
import random
import pandas as pd
from datetime import datetime, timedelta, date
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('fmcg_simulator.log')
    ]
)
logger = logging.getLogger(__name__)

def table_has_data(client, table_id):
    """Check if a BigQuery table has data"""
    try:
        logger.info(f"Checking if table {table_id} has data...")
        df = client.query(f"SELECT COUNT(1) AS cnt FROM `{table_id}` LIMIT 1").to_dataframe()
        has_data = df['cnt'][0] > 0
        logger.info(f"Table {table_id} has data: {has_data}")
        return has_data
    except Exception as e:
        logger.error(f"Error checking table {table_id}: {str(e)}")
        return False

def append_df_bq(client, df, table_id, write_disposition="WRITE_APPEND"):
    """Append DataFrame to BigQuery table with proper null handling for Power BI compatibility"""
    logger.info(f"Preparing to load {len(df):,} rows into {table_id}...")
    
    try:
        # Ensure proper null handling - pandas will convert pd.NaT and None to NULL in BigQuery
        df = df.copy()
        
        # Replace pd.NaT in non-datetime columns with None for better compatibility
        for col in df.columns:
            if df[col].dtype == 'object':  # String/object columns
                df[col] = df[col].replace({pd.NaT: None})
        
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition, autodetect=True)
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logger.info(f"✓ Loaded {len(df):,} rows → {table_id}")
        
    except Exception as e:
        logger.error(f"Failed to load data into {table_id}: {str(e)}")
        raise

def append_df_bq_safe(client, df, table_id, id_column, write_disposition="WRITE_APPEND"):
    """
    Append DataFrame to BigQuery table with duplicate ID prevention.
    Checks for existing IDs and only appends unique records.
    
    Args:
        client: BigQuery client
        df: DataFrame to append
        table_id: Full BigQuery table ID (project.dataset.table)
        id_column: Name of the ID column to check for duplicates (e.g., 'Product ID')
        write_disposition: BigQuery write disposition (default: WRITE_APPEND)
    
    Returns:
        Number of rows actually appended (after filtering duplicates)
    """
    if df.empty:
        logger.warning(f"No data to append to {table_id}")
        return 0
    
    logger.info(f"Checking for duplicates in {table_id} using {id_column}...")
    df = df.copy()
    
    # Replace pd.NaT in non-datetime columns with None for better compatibility
    for col in df.columns:
        if df[col].dtype == 'object':  # String/object columns
            df[col] = df[col].replace({pd.NaT: None})
    
    # Check if ID column exists
    if id_column not in df.columns:
        error_msg = f"ID column '{id_column}' not found in DataFrame columns: {list(df.columns)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    original_count = len(df)
    
    # Query existing IDs from BigQuery
    try:
        # Get list of IDs to check
        ids_to_check = df[id_column].unique().tolist()
        
        if ids_to_check:
            # Build SQL query with proper escaping for IN clause
            escaped_ids = [str(id_val).replace("'", "''") for id_val in ids_to_check]
            id_list_str = ", ".join([f"'{id_val}'" for id_val in escaped_ids])
            
            existing_ids_query = f"""
                SELECT DISTINCT `{id_column}` as existing_id
                FROM `{table_id}`
                WHERE `{id_column}` IN ({id_list_str})
                LIMIT 1000
            """
            
            existing_ids_df = client.query(existing_ids_query).to_dataframe()
            
            if not existing_ids_df.empty:
                existing_ids_set = set(existing_ids_df['existing_id'].tolist())
                duplicate_count = len(df[df[id_column].isin(existing_ids_set)])
                
                if duplicate_count > 0:
                    logger.warning(f"Found {duplicate_count:,} duplicate {id_column}(s). Filtering them out...")
                    # Filter out rows with existing IDs
                    df = df[~df[id_column].isin(existing_ids_set)]
                    
                    if df.empty:
                        logger.warning(f"All records were duplicates. Nothing to append to {table_id}")
                        return 0
                    
                    logger.info(f"✓ Filtered to {len(df):,} unique records (removed {duplicate_count:,} duplicates)")
    except Exception as e:
        # If table doesn't exist or query fails, proceed with append (first load scenario)
        if "Not found" in str(e) or "does not exist" in str(e).lower():
            logger.info(f"Table doesn't exist yet or is empty. Proceeding with initial load...")
        else:
            # For other errors, log but proceed (better to try than fail)
            logger.warning(f"Could not check for duplicates: {str(e)}. Proceeding with append...")
    
    # Append the filtered dataframe
    if not df.empty:
        try:
            job_config = bigquery.LoadJobConfig(write_disposition=write_disposition, autodetect=True)
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            logger.info(f"✓ Loaded {len(df):,} rows → {table_id}")
            return len(df)
        except Exception as e:
            logger.error(f"Failed to load data into {table_id}: {str(e)}")
            raise
    else:
        return 0

def random_date(start_year=2015, end_year=2025):
    """Generate random date between years"""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end-start).days))

def random_birth_date(min_age=20, max_age=65):
    """Generate random birth date based on age range"""
    today = datetime.now()
    birth_year = today.year - random.randint(min_age, max_age)
    return datetime(birth_year, random.randint(1,12), random.randint(1,28))

def random_termination_date(hire_date):
    """Generate random termination date after hire date"""
    min_date = hire_date + timedelta(days=30)
    max_date = datetime.now()
    if min_date > max_date:
        return None
    return min_date + timedelta(days=random.randint(0, (max_date-min_date).days))

def random_date_range(start_date, end_date):
    """Generate random date between start_date and end_date (inclusive)"""
    if isinstance(start_date, date) and not isinstance(start_date, datetime):
        start_date = datetime.combine(start_date, datetime.min.time())
    if isinstance(end_date, date) and not isinstance(end_date, datetime):
        end_date = datetime.combine(end_date, datetime.min.time())
    delta = (end_date - start_date).days
    if delta < 0:
        delta = 0
    return start_date + timedelta(days=random.randint(0, delta))

def update_delivery_status(client, fact_sales_table):
    """Check and report delivery status without DML operations (free tier compatible)"""
    logger.info("="*60)
    logger.info("DELIVERY STATUS MONITOR")
    logger.info("="*60)
    
    try:
        today = datetime.now().date()
        logger.info(f"Checking delivery status for orders as of {today}")
        
        # Read-only query to check orders that should be delivered
        # This identifies orders that would be updated in a full version
        check_query = f"""
            SELECT 
                `sale_key`,
                `sale_date`,
                `expected_delivery_date`,
                `delivery_status`,
                `total_amount`,
                `retailer_key`,
                CASE 
                    WHEN `delivery_status` IN ('In Transit', 'Processing')
                    AND `expected_delivery_date` <= '{today}'
                    AND `actual_delivery_date` IS NULL
                    THEN 'Should be Delivered'
                    ELSE 'Current Status'
                END as action_needed
            FROM `{fact_sales_table}`
            WHERE `sale_date` >= DATE_SUB('{today}', INTERVAL 30 DAY)
            ORDER BY `expected_delivery_date` ASC
            LIMIT 100
        """
        
        # Execute the read-only query
        result_df = client.query(check_query).to_dataframe()
        
        if result_df.empty:
            logger.info("No recent orders found in the last 30 days.")
            return
        
        # Count orders by status and action needed
        status_summary = result_df.groupby(['delivery_status', 'action_needed']).agg({
            'sale_key': 'count',
            'total_amount': 'sum'
        }).rename(columns={'sale_key': 'count'})
        
        logger.info(f"Found {len(result_df):,} recent orders:")
        
        for (delivery_status, action_needed), group in status_summary.iterrows():
            count = int(group['count'])
            amount = float(group['total_amount'])
            logger.info(f"  {delivery_status} - {action_needed}: {count:,} orders (PHP {amount:,.2f})")
        
        # Identify orders that need delivery status update
        needs_update = result_df[result_df['action_needed'] == 'Should be Delivered']
        
        if not needs_update.empty:
            logger.info(f"\nOrders Ready for Delivery ({len(needs_update)} orders):")
            total_value = needs_update['total_amount'].sum()
            logger.info(f"   Total Value: PHP {total_value:,.2f}")
            logger.info(f"   Date Range: {needs_update['expected_delivery_date'].min()} to {needs_update['expected_delivery_date'].max()}")
            
            # Show sample of orders that would be updated
            sample_size = min(5, len(needs_update))
            logger.info(f"\nSample Orders (showing {sample_size} of {len(needs_update)}):")
            for _, order in needs_update.head(sample_size).iterrows():
                # Convert string dates to date objects if needed
                expected_date = pd.to_datetime(order['expected_delivery_date']).date()
                days_overdue = (today - expected_date).days
                status_text = f"{days_overdue} days overdue" if days_overdue > 0 else "due today"
                logger.info(f"   Order #{order['sale_key']}: PHP {order['total_amount']:,.2f} ({status_text})")
        else:
            logger.info("✅ All deliveries are current. No orders need status updates.")
        
        # Overall delivery status summary
        overall_summary = result_df.groupby('delivery_status').agg({
            'sale_key': 'count',
            'total_amount': 'sum'
        }).rename(columns={'sale_key': 'count'})
        
        logger.info(f"\nOverall Delivery Status Summary:")
        for status, group in overall_summary.iterrows():
            count = int(group['count'])
            amount = float(group['total_amount'])
            percentage = (count / len(result_df)) * 100
            logger.info(f"   {status}: {count:,} orders ({percentage:.1f}%) - PHP {amount:,.2f}")
        
        logger.info(f"\nNote: This is a read-only monitoring version.")
        logger.info(f"   To update delivery status, enable billing or use a paid BigQuery tier.")
        
    except Exception as e:
        logger.error(f"Error checking delivery status: {str(e)}")
        # Don't raise the error - just log it so the scheduled run can continue
        logger.info("Continuing with scheduled run despite delivery status check failure...")
