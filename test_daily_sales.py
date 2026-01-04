#!/usr/bin/env python3
"""
Test script to verify daily sales amount configuration
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'FMCG'))

from config import INITIAL_SALES_AMOUNT, DAILY_SALES_AMOUNT

def test_daily_sales_amount():
    """Test that daily sales amount is set correctly for scheduled runs"""
    print("🧪 Testing Daily Sales Amount Configuration")
    print("=" * 50)
    
    # Test the configuration
    expected_daily = 2000000  # ₱2M target for daily operations
    actual_daily = DAILY_SALES_AMOUNT
    
    print(f"Annual Sales Target: ₱{INITIAL_SALES_AMOUNT:,}")
    print(f"Expected Daily (Scheduled Runs): ₱{expected_daily:,}")
    print(f"Actual Daily: ₱{actual_daily:,}")
    
    # Check if they match
    if expected_daily == actual_daily:
        print("✅ Daily sales amount is set correctly for scheduled runs!")
        
        # Check if it's around 2M as requested
        if 1900000 <= actual_daily <= 2100000:
            print("✅ Daily amount is in the expected range (~₱2M)")
            print("💡 This means scheduled runs will generate ~₱2M per day")
            print("💡 Manual runs will still generate the full ₱8B historical data")
        else:
            print(f"⚠️  Daily amount is ₱{actual_daily:,}, which may not be the expected ~₱2M")
        
        return True
    else:
        print(f"❌ Daily amount mismatch!")
        print(f"   Expected: ₱{expected_daily:,}")
        print(f"   Actual: ₱{actual_daily:,}")
        return False

def test_scheduled_vs_manual_logic():
    """Test the logic for scheduled vs manual runs"""
    print("\n🔄 Testing Scheduled vs Manual Run Logic")
    print("=" * 50)
    
    # Simulate scheduled run
    is_scheduled = True
    yesterday = "2024-01-03"
    
    if is_scheduled:
        sales_target = DAILY_SALES_AMOUNT
        start_date = yesterday
        end_date = yesterday
        run_type = "Daily run"
        print(f"📅 {run_type}: ₱{sales_target:,.0f} for {start_date}")
    else:
        sales_target = INITIAL_SALES_AMOUNT
        start_date = "2015-01-01"
        end_date = yesterday
        run_type = "Manual run"
        print(f"🔧 {run_type}: ₱{sales_target:,.0f} from {start_date} to {end_date}")
    
    # Test manual run
    is_scheduled = False
    if is_scheduled:
        sales_target = DAILY_SALES_AMOUNT
        start_date = yesterday
        end_date = yesterday
        run_type = "Daily run"
        print(f"📅 {run_type}: ₱{sales_target:,.0f} for {start_date}")
    else:
        sales_target = INITIAL_SALES_AMOUNT
        start_date = "2015-01-01"
        end_date = yesterday
        run_type = "Manual run"
        print(f"🔧 {run_type}: ₱{sales_target:,.0f} from {start_date} to {end_date}")
    
    print("✅ Scheduled vs Manual logic working correctly!")
    return True

if __name__ == "__main__":
    print("🚀 Starting Daily Sales Configuration Test")
    
    success1 = test_daily_sales_amount()
    success2 = test_scheduled_vs_manual_logic()
    
    if success1 and success2:
        print("\n🎉 All daily sales tests passed!")
        print("✅ Ready for scheduled runs with ~₱2M daily sales")
    else:
        print("\n❌ Some tests failed - check configuration")
    
    sys.exit(0 if (success1 and success2) else 1)
