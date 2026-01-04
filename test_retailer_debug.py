#!/usr/bin/env python3
"""
Test script to isolate and debug the retailer generation issue
"""

import sys
import os
import time
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'FMCG'))

from generators.dimensional import generate_dim_locations, generate_dim_retailers_normalized
from helpers import append_df_bq
from auth import get_bigquery_client
from config import INITIAL_RETAILERS, PROJECT_ID, DATASET, DIM_RETAILERS

def test_retailer_generation():
    """Test retailer generation and BigQuery upload in isolation"""
    print("🧪 Testing Retailer Generation Isolated")
    print("=" * 50)
    
    try:
        # Step 1: Generate locations (needed for retailers)
        print("\n📍 Step 1: Generating locations...")
        start_time = time.time()
        locations = generate_dim_locations(num_locations=100)
        print(f"✅ Generated {len(locations)} locations in {time.time() - start_time:.2f}s")
        
        # Step 2: Generate retailers
        print("\n🏪 Step 2: Generating retailers...")
        start_time = time.time()
        retailers = generate_dim_retailers_normalized(
            num_retailers=INITIAL_RETAILERS,
            locations=locations
        )
        print(f"✅ Generated {len(retailers)} retailers in {time.time() - start_time:.2f}s")
        
        # Show sample retailer
        if retailers:
            sample = retailers[0]
            print(f"📋 Sample retailer: {sample['retailer_name']} ({sample['retailer_type']})")
        
        # Step 3: Test BigQuery connection
        print("\n🔐 Step 3: Testing BigQuery connection...")
        start_time = time.time()
        client = get_bigquery_client(PROJECT_ID)
        print(f"✅ Connected to BigQuery in {time.time() - start_time:.2f}s")
        
        # Step 4: Test table existence check
        print("\n🔍 Step 4: Checking table existence...")
        start_time = time.time()
        table_exists = False
        try:
            client.get_table(DIM_RETAILERS)
            table_exists = True
            print(f"✅ Table {DIM_RETAILERS} exists")
        except Exception:
            print(f"✅ Table {DIM_RETAILERS} does not exist (expected)")
        print(f"Table check completed in {time.time() - start_time:.2f}s")
        
        # Step 5: Test BigQuery upload (with timeout monitoring)
        print("\n📤 Step 5: Testing BigQuery upload...")
        print(f"⏱️  Starting upload at {time.strftime('%H:%M:%S')}")
        
        import pandas as pd
        start_time = time.time()
        
        # Monitor the upload process
        try:
            append_df_bq(client, pd.DataFrame(retailers), DIM_RETAILERS)
            elapsed = time.time() - start_time
            print(f"✅ Upload completed successfully in {elapsed:.2f}s")
            
            # Verify upload
            print("\n🔍 Step 6: Verifying upload...")
            result = client.query(f"SELECT COUNT(*) as count FROM `{DIM_RETAILERS}`").to_dataframe()
            print(f"✅ Verification: {result['count'][0]} rows in table")
            
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"❌ Upload failed after {elapsed:.2f}s: {str(e)}")
            if "timeout" in str(e).lower():
                print("⏰ This appears to be a timeout issue")
                print("💡 Suggestions:")
                print("   1. Check network connectivity")
                print("   2. Verify BigQuery quota limits")
                print("   3. Try smaller batch sizes")
            return False
        
        print("\n🎉 Retailer generation test completed successfully!")
        return True
        
    except Exception as e:
        print(f"\n💥 Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_retailer_generation()
    if success:
        print("\n✅ Retailer generation is working correctly")
    else:
        print("\n❌ Retailer generation has issues that need to be addressed")
    sys.exit(0 if success else 1)
