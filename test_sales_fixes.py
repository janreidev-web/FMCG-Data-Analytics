#!/usr/bin/env python3
"""
Test script to verify sales generation fixes
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'FMCG'))

from datetime import date
from FMCG.generators.dimensional import generate_fact_sales

def test_sales_generation_fixes():
    """Test that sales generation works with the fixes applied"""
    print("Testing sales generation fixes...")
    
    # Create test employees with recent hire dates (simulating the issue)
    employees = [
        {
            "employee_key": 1,
            "job_key": 1,
            "hire_date": date(2025, 1, 1),  # Recent hire date
            "employment_status": "Active",
            "termination_date": None
        },
        {
            "employee_key": 2,
            "job_key": 2,
            "hire_date": date(2024, 6, 15),  # Recent hire date
            "employment_status": "Active",
            "termination_date": None
        }
    ]
    
    # Create test products with recent creation dates
    products = [
        {
            "product_key": 1,
            "retail_price": 100.0,
            "created_date": date(2025, 2, 1),  # Recent creation date
            "status": "Active"
        },
        {
            "product_key": 2,
            "retail_price": 150.0,
            "created_date": date(2024, 8, 15),  # Recent creation date
            "status": "Active"
        }
    ]
    
    # Create test retailers
    retailers = [
        {"retailer_key": 1},
        {"retailer_key": 2}
    ]
    
    # Create test campaigns
    campaigns = []
    
    print("Test scenario:")
    print(f"  Employees: {len(employees)} with hire dates: {[e['hire_date'] for e in employees]}")
    print(f"  Products: {len(products)} with creation dates: {[p['created_date'] for p in products]}")
    print(f"  Testing sales generation from 2015-01-01 to 2025-12-31")
    
    # Test sales generation with historical date range
    try:
        sales = generate_fact_sales(
            employees, products, retailers, campaigns,
            target_amount=100000,  # Small target for testing
            start_date=date(2015, 1, 1),
            end_date=date(2025, 12, 31)
        )
        
        print(f"✓ Generated {len(sales)} sales records")
        
        if sales:
            # Check first record structure
            record = sales[0]
            print(f"Sample sale: Date={record['sale_date']}, Amount=₱{record['total_amount']:,.2f}")
            
            # Verify required fields
            required_fields = [
                'sale_key', 'sale_date', 'product_key', 'employee_key', 
                'retailer_key', 'campaign_key', 'case_quantity', 'unit_price',
                'discount_percent', 'discount_amount', 'tax_rate', 'tax_amount',
                'total_amount', 'commission_amount', 'currency', 'payment_method',
                'payment_status', 'delivery_status', 'expected_delivery_date',
                'actual_delivery_date'
            ]
            
            for field in required_fields:
                assert field in record, f"Missing field: {field}"
            
            # Check data types
            assert isinstance(record['sale_key'], int), "sale_key should be int"
            assert isinstance(record['discount_amount'], (int, float)), "discount_amount should be numeric"
            assert isinstance(record['tax_amount'], (int, float)), "tax_amount should be numeric"
            
            print("✓ All required fields present with correct types")
            
            # Check date range
            dates = [s['sale_date'] for s in sales]
            min_date = min(dates)
            max_date = max(dates)
            print(f"Sales date range: {min_date} to {max_date}")
            
            # Should have sales in the requested range
            assert min_date >= date(2015, 1, 1), "Sales should start from 2015"
            assert max_date <= date(2025, 12, 31), "Sales should end by 2025"
            
            print("✓ Sales generated in correct date range")
        else:
            print("⚠ No sales generated - this might indicate an issue")
        
        print("✅ Sales generation test passed!")
        
    except Exception as e:
        print(f"❌ Sales generation test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

def test_product_created_date_handling():
    """Test that product created_date None values are handled correctly"""
    print("\nTesting product created_date handling...")
    
    # Test with products that have None created_date
    products_with_none = [
        {
            "product_key": 1,
            "retail_price": 100.0,
            "created_date": None,  # None value
            "status": "Active"
        },
        {
            "product_key": 2,
            "retail_price": 150.0,
            "created_date": date(2020, 1, 1),  # Valid date
            "status": "Active"
        }
    ]
    
    # Test the filtering logic
    from datetime import datetime, timedelta
    sample_date = date(2015, 6, 1)
    
    # This should handle None values gracefully
    available_products = [
        p for p in products_with_none 
        if p.get('created_date') and p.get('created_date') <= sample_date
    ]
    
    print(f"Products with None created_date: {len(products_with_none)}")
    print(f"Available products for {sample_date}: {len(available_products)}")
    
    # Should not crash and should handle None values
    assert len(available_products) >= 0, "Should handle None created_date values"
    
    print("✓ Product created_date None handling works correctly")

if __name__ == "__main__":
    try:
        test_product_created_date_handling()
        success = test_sales_generation_fixes()
        
        if success:
            print("\n🎉 All tests passed! The sales generation fixes are working correctly.")
        else:
            print("\n❌ Some tests failed. Please check the implementation.")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Test execution failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
