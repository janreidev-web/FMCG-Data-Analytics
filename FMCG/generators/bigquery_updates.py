"""
BigQuery Free Tier Update Methods
Implements the three official methods for updating data in BigQuery free tier
"""
import pandas as pd
from datetime import date, timedelta


def create_delivery_update_query(project_id, dataset, sales_table='fact_sales'):
    """
    Method 1: Overwrite table using WRITE_TRUNCATE
    Creates a query that updates delivery statuses and dates based on time logic
    """
    return f"""
    -- Update delivery statuses and dates using time-based progression
    SELECT 
        sale_key,
        sale_date,
        product_key,
        employee_key,
        retailer_key,
        campaign_key,
        case_quantity,
        unit_price,
        discount_percent,
        discount_amount,
        tax_rate,
        tax_amount,
        total_amount,
        commission_amount,
        currency,
        payment_method,
        payment_status,
        expected_delivery_date,
        -- Updated delivery status and actual delivery date based on time
        CASE 
            WHEN delivery_status = 'Pending' AND DATE_DIFF(CURRENT_DATE(), sale_date, DAY) >= 1 THEN 'Processing'
            WHEN delivery_status = 'Processing' AND DATE_DIFF(CURRENT_DATE(), sale_date, DAY) >= 2 THEN 'In Transit'
            WHEN delivery_status = 'In Transit' AND DATE_DIFF(CURRENT_DATE(), sale_date, DAY) >= 4 THEN 'Delivered'
            ELSE delivery_status
        END as delivery_status,
        -- Set actual delivery date when status becomes Delivered
        CASE 
            WHEN delivery_status = 'In Transit' AND DATE_DIFF(CURRENT_DATE(), sale_date, DAY) >= 4 THEN CURRENT_DATE()
            WHEN delivery_status = 'Delivered' THEN actual_delivery_date
            ELSE NULL
        END as actual_delivery_date
    FROM `{project_id}.{dataset}.{sales_table}`
    WHERE sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    """


def create_delivery_update_with_new_data_query(project_id, dataset, sales_table='fact_sales'):
    """
    Method 2: Append new data using WRITE_APPEND
    Creates new delivery status update records with delivery dates
    """
    return f"""
    -- Generate delivery status updates as new records with delivery dates
    SELECT 
        ROW_NUMBER() OVER (ORDER BY sale_key, DATE_DIFF(CURRENT_DATE(), sale_date, DAY)) as update_key,
        sale_key,
        delivery_status as previous_status,
        CASE 
            WHEN delivery_status = 'Pending' AND DATE_DIFF(CURRENT_DATE(), sale_date, DAY) >= 1 THEN 'Processing'
            WHEN delivery_status = 'Processing' AND DATE_DIFF(CURRENT_DATE(), sale_date, DAY) >= 2 THEN 'In Transit'
            WHEN delivery_status = 'In Transit' AND DATE_DIFF(CURRENT_DATE(), sale_date, DAY) >= 4 THEN 'Delivered'
            ELSE delivery_status
        END as new_status,
        actual_delivery_date as previous_actual_delivery_date,
        CASE 
            WHEN delivery_status = 'In Transit' AND DATE_DIFF(CURRENT_DATE(), sale_date, DAY) >= 4 THEN CURRENT_DATE()
            WHEN delivery_status = 'Delivered' THEN actual_delivery_date
            ELSE NULL
        END as new_actual_delivery_date,
        CURRENT_DATE() as update_date,
        DATE_DIFF(CURRENT_DATE(), sale_date, DAY) as days_since_sale,
        CASE 
            WHEN delivery_status = 'Pending' AND DATE_DIFF(CURRENT_DATE(), sale_date, DAY) >= 1 THEN 'Order processed after 1 day(s)'
            WHEN delivery_status = 'Processing' AND DATE_DIFF(CURRENT_DATE(), sale_date, DAY) >= 2 THEN 'Shipped after 2 day(s) in processing'
            WHEN delivery_status = 'In Transit' AND DATE_DIFF(CURRENT_DATE(), sale_date, DAY) >= 4 THEN 'Delivered after 4 day(s) in transit'
            ELSE 'No update needed'
        END as update_reason
    FROM `{project_id}.{dataset}.{sales_table}`
    WHERE delivery_status IN ('Pending', 'Processing', 'In Transit')
    AND sale_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND (
        (delivery_status = 'Pending' AND DATE_DIFF(CURRENT_DATE(), sale_date, DAY) >= 1) OR
        (delivery_status = 'Processing' AND DATE_DIFF(CURRENT_DATE(), sale_date, DAY) >= 2) OR
        (delivery_status = 'In Transit' AND DATE_DIFF(CURRENT_DATE(), sale_date, DAY) >= 4)
    )
    """


def create_staging_table_update_query(project_id, dataset, sales_table='fact_sales', staging_table='delivery_updates_staging'):
    """
    Method 3: Using staging table for complex updates
    Creates a query that combines main table with staging table updates
    """
    return f"""
    -- Combine original sales with staging table updates
    WITH latest_updates AS (
        SELECT 
            sale_key,
            new_status,
            update_date,
            ROW_NUMBER() OVER (PARTITION BY sale_key ORDER BY update_date DESC) as rn
        FROM `{project_id}.{dataset}.{staging_table}`
    )
    SELECT 
        COALESCE(u.sale_key, s.sale_key) as sale_key,
        s.sale_date,
        s.product_key,
        s.employee_key,
        s.retailer_key,
        s.campaign_key,
        s.case_quantity,
        s.unit_price,
        s.discount_percent,
        s.discount_amount,
        s.tax_rate,
        s.tax_amount,
        s.total_amount,
        s.commission_amount,
        s.currency,
        s.payment_method,
        s.payment_status,
        -- Use updated status if available, otherwise original
        COALESCE(u.new_status, s.delivery_status) as delivery_status
    FROM `{project_id}.{dataset}.{sales_table}` s
    LEFT JOIN latest_updates u ON s.sale_key = u.sale_key AND u.rn = 1
    """


def execute_method_1_overwrite(client, project_id, dataset, sales_table='fact_sales'):
    """
    Execute Method 1: Overwrite table with updated delivery statuses
    """
    query = create_delivery_update_query(project_id, dataset, sales_table)
    
    job_config = {
        'destination': f'{project_id}.{dataset}.{sales_table}',
        'write_disposition': 'WRITE_TRUNCATE'  # Overwrite existing table
    }
    
    try:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for completion
        
        print(f"✅ Method 1: Updated delivery statuses in {sales_table}")
        print(f"   Affected rows: {query_job.num_dml_affected_rows if hasattr(query_job, 'num_dml_affected_rows') else 'Unknown'}")
        
        return True
    except Exception as e:
        print(f"❌ Method 1 failed: {e}")
        return False


def execute_method_2_append(client, project_id, dataset, updates_table='delivery_status_updates'):
    """
    Execute Method 2: Append new delivery update records
    """
    query = create_delivery_update_with_new_data_query(project_id, dataset)
    
    job_config = {
        'destination': f'{project_id}.{dataset}.{updates_table}',
        'write_disposition': 'WRITE_APPEND'  # Append to existing table
    }
    
    try:
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for completion
        
        rows_updated = query_job.num_dml_affected_rows if hasattr(query_job, 'num_dml_affected_rows') else 0
        print(f"✅ Method 2: Appended {rows_updated} delivery status updates to {updates_table}")
        
        return True
    except Exception as e:
        print(f"❌ Method 2 failed: {e}")
        return False


def execute_method_3_staging(client, project_id, dataset, sales_table='fact_sales', staging_table='delivery_updates_staging'):
    """
    Execute Method 3: Use staging table for complex updates
    """
    # First, populate staging table with updates
    staging_query = create_delivery_update_with_new_data_query(project_id, dataset)
    
    try:
        # Clear staging table and load new updates
        staging_job_config = {
            'destination': f'{project_id}.{dataset}.{staging_table}',
            'write_disposition': 'WRITE_TRUNCATE'
        }
        
        staging_job = client.query(staging_query, job_config=staging_job_config)
        staging_job.result()
        
        print(f"✅ Staging table populated with updates")
        
        # Now overwrite main table using staging data
        final_query = create_staging_table_update_query(project_id, dataset, sales_table, staging_table)
        
        final_job_config = {
            'destination': f'{project_id}.{dataset}.{sales_table}',
            'write_disposition': 'WRITE_TRUNCATE'
        }
        
        final_job = client.query(final_query, job_config=final_job_config)
        final_job.result()
        
        print(f"✅ Method 3: Updated {sales_table} using staging table {staging_table}")
        
        return True
    except Exception as e:
        print(f"❌ Method 3 failed: {e}")
        return False


def create_current_delivery_status_view(project_id, dataset, sales_table='fact_sales', updates_table='delivery_status_updates'):
    """
    Create a view that shows current delivery status using Method 2 approach
    Includes delivery dates for comprehensive tracking
    """
    view_query = f"""
    CREATE OR REPLACE VIEW `{project_id}.{dataset}.current_delivery_status` AS
    SELECT 
        s.sale_key,
        s.sale_date,
        s.product_key,
        s.employee_key,
        s.retailer_key,
        s.campaign_key,
        s.case_quantity,
        s.unit_price,
        s.discount_percent,
        s.discount_amount,
        s.tax_rate,
        s.tax_amount,
        s.total_amount,
        s.commission_amount,
        s.currency,
        s.payment_method,
        s.payment_status,
        s.expected_delivery_date,
        -- Get latest delivery status from updates table, fallback to original
        COALESCE(
            latest_update.new_status, 
            s.delivery_status
        ) as current_delivery_status,
        -- Get latest actual delivery date
        COALESCE(
            latest_update.actual_delivery_date,
            s.actual_delivery_date
        ) as current_actual_delivery_date,
        COALESCE(
            latest_update.update_date,
            s.sale_date
        ) as last_status_update_date,
        latest_update.update_reason,
        -- Calculate delivery performance
        CASE 
            WHEN COALESCE(latest_update.new_status, s.delivery_status) = 'Delivered' THEN 
                DATE_DIFF(COALESCE(latest_update.actual_delivery_date, s.actual_delivery_date, CURRENT_DATE()), s.sale_date, DAY)
            ELSE 
                DATE_DIFF(CURRENT_DATE(), s.sale_date, DAY)
        END as days_since_sale,
        -- Check if delivery is on time
        CASE 
            WHEN COALESCE(latest_update.new_status, s.delivery_status) = 'Delivered' AND
                 COALESCE(latest_update.actual_delivery_date, s.actual_delivery_date) <= s.expected_delivery_date 
                 THEN 'On Time'
            WHEN COALESCE(latest_update.new_status, s.delivery_status) = 'Delivered' THEN 'Late'
            WHEN CURRENT_DATE() > s.expected_delivery_date THEN 'Overdue'
            ELSE 'On Schedule'
        END as delivery_performance
    FROM `{project_id}.{dataset}.{sales_table}` s
    LEFT JOIN (
        SELECT 
            du.*,
            ROW_NUMBER() OVER (PARTITION BY sale_key ORDER BY update_date DESC, update_key DESC) as rn
        FROM `{project_id}.{dataset}.{updates_table}` du
    ) latest_update ON s.sale_key = latest_update.sale_key AND latest_update.rn = 1
    """
    
    return view_query


def get_delivery_status_summary(client, project_id, dataset):
    """
    Get summary of current delivery statuses
    """
    summary_query = f"""
    SELECT 
        current_delivery_status,
        COUNT(*) as order_count,
        SUM(total_amount) as total_value,
        AVG(DATE_DIFF(CURRENT_DATE(), sale_date, DAY)) as avg_days_since_sale
    FROM `{project_id}.{dataset}.current_delivery_status`
    GROUP BY current_delivery_status
    ORDER BY order_count DESC
    """
    
    try:
        result_df = client.query(summary_query).to_dataframe()
        return result_df
    except Exception as e:
        print(f"Error getting status summary: {e}")
        return pd.DataFrame()


def compare_update_methods(client, project_id, dataset, sales_table='fact_sales'):
    """
    Compare the three update methods and recommend the best approach
    """
    print("🔍 Comparing BigQuery Free Tier Update Methods")
    print("=" * 60)
    
    # Method 1: Direct overwrite
    print("\n📊 Method 1: Direct Table Overwrite")
    print("   Pros: Simple, no extra tables, immediate updates")
    print("   Cons: No audit trail, loses original data")
    print("   Best for: Simple status updates, when history isn't needed")
    
    # Method 2: Append updates
    print("\n📊 Method 2: Append Update Records")
    print("   Pros: Complete audit trail, preserves original data")
    print("   Cons: Requires view for current status, extra storage")
    print("   Best for: When you need full history and analytics")
    
    # Method 3: Staging table
    print("\n📊 Method 3: Staging Table Approach")
    print("   Pros: Most flexible, can handle complex logic")
    print("   Cons: Most complex, requires temporary table")
    print("   Best for: Complex updates with multiple conditions")
    
    # Get current table size for recommendation
    try:
        size_query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNTIF(delivery_status IN ('Pending', 'Processing', 'In Transit')) as active_orders
        FROM `{project_id}.{dataset}.{sales_table}`
        """
        
        size_result = client.query(size_query).to_dataframe()
        total_rows = size_result['total_rows'].iloc[0]
        active_orders = size_result['active_orders'].iloc[0]
        
        print(f"\n📈 Current Table Analysis:")
        print(f"   Total rows: {total_rows:,}")
        print(f"   Active orders: {active_orders:,}")
        
        # Recommendation
        if active_orders < 1000:
            recommendation = "Method 1 (Direct Overwrite) - Simple and efficient for small datasets"
        elif active_orders < 10000:
            recommendation = "Method 2 (Append Updates) - Good balance of features and performance"
        else:
            recommendation = "Method 3 (Staging Table) - Most efficient for large datasets"
        
        print(f"\n🎯 Recommended Method: {recommendation}")
        
    except Exception as e:
        print(f"\n⚠️  Could not analyze table size: {e}")
        print("🎯 Default Recommendation: Method 2 (Append Updates) - Most versatile")
