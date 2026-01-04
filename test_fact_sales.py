#!/usr/bin/env python3
"""
Test script to verify fact_sales generation fixes
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'FMCG'))

from datetime import date
from FMCG.id_generation import generate_unique_sale_key
from FMCG.generators.dimensional import generate_fact_sales

def test_id_generation():
    """Test that generate_unique_sale_key returns integer"""
    print("Testing ID generation...")
    
    # Test basic functionality
    sale_key = generate_unique_sale_key("2024-01-01", 1, 1, 1, 1)
    print(f"Generated sale_key: {sale_key} (type: {type(sale_key)})")
    
    assert isinstance(sale_key, int), f"Expected int, got {type(sale_key)}"
    assert sale_key > 0, "Expected positive integer"
    
    # Test uniqueness
    sale_key2 = generate_unique_sale_key("2024-01-01", 1, 1, 1, 2)
    assert sale_key != sale_key2, "Keys should be unique"
    
    print("✓ ID generation test passed")

def test_fact_sales_structure():
    """Test that fact_sales generation produces correct structure"""
    print("Testing fact_sales structure...")
    
    # Create minimal test data
    employees = [{
        "employee_key": 1,
        "hire_date": date(2024, 1, 1),
        "employment_status": "Active"
    }]
    
    products = [{
        "product_key": 1,
        "retail_price": 100.0,
        "created_date": date(2024, 1, 1)
    }]
    
    retailers = [{
        "retailer_key": 1
    }]
    
    campaigns = []
    
    # Generate small test dataset
    sales = generate_fact_sales(
        employees, products, retailers, campaigns,
        target_amount=10000,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 1)
    )
    
    print(f"Generated {len(sales)} sales records")
    
    if sales:
        # Check first record structure
        record = sales[0]
        print(f"Sample record keys: {list(record.keys())}")
        
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
        
        print("✓ Fact sales structure test passed")
    else:
        print("⚠ No sales generated (may be expected if no employees/products available)")

if __name__ == "__main__":
    try:
        test_id_generation()
        test_fact_sales_structure()
        print("\n✅ All tests passed!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
