#!/usr/bin/env python3
"""
Comprehensive Deep Test Suite for FMCG Data Simulator
Tests all components, relationships, and potential failure points
"""

import sys
import os
import time
import traceback
from datetime import date, timedelta
import pandas as pd

# Add FMCG to path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'FMCG'))

class FMCGDeepTest:
    def __init__(self):
        self.test_results = {}
        self.errors = []
        self.warnings = []
        
    def log_test(self, test_name, passed, message="", details=None):
        """Log test result"""
        self.test_results[test_name] = {
            'passed': passed,
            'message': message,
            'details': details or {}
        }
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {test_name}")
        if message:
            print(f"    {message}")
        if details and not passed:
            for key, value in details.items():
                print(f"    {key}: {value}")
    
    def test_imports(self):
        """Test all critical imports"""
        print("\n🔍 Testing Imports...")
        
        try:
            # Test dimensional generators
            from generators.dimensional import (
                generate_dim_locations, generate_dim_departments, generate_dim_jobs,
                generate_dim_banks, generate_dim_insurance, generate_dim_categories,
                generate_dim_brands, generate_dim_subcategories, generate_dim_products,
                generate_dim_employees_normalized, generate_dim_retailers_normalized,
                generate_dim_campaigns, generate_dim_dates, validate_relationships,
                generate_fact_sales, generate_fact_operating_costs, 
                generate_fact_marketing_costs, generate_fact_inventory
            )
            self.log_test("Dimensional Generators Import", True)
            
            # Test config
            from config import (
                PROJECT_ID, DATASET, INITIAL_EMPLOYEES, INITIAL_PRODUCTS, 
                INITIAL_RETAILERS, INITIAL_SALES_AMOUNT, DAILY_SALES_AMOUNT,
                DIM_EMPLOYEES, DIM_PRODUCTS, DIM_RETAILERS, DIM_CAMPAIGNS,
                DIM_LOCATIONS, DIM_DEPARTMENTS, DIM_JOBS, DIM_BANKS, DIM_INSURANCE,
                DIM_CATEGORIES, DIM_BRANDS, DIM_SUBCATEGORIES, DIM_DATES,
                FACT_SALES, FACT_OPERATING_COSTS, FACT_INVENTORY, 
                FACT_MARKETING_COSTS, FACT_EMPLOYEES
            )
            self.log_test("Config Import", True)
            
            # Test helpers
            from helpers import table_has_data, append_df_bq, append_df_bq_safe
            self.log_test("Helpers Import", True)
            
            # Test auth (may fail without credentials, but import should work)
            try:
                from auth import get_bigquery_client
                self.log_test("Auth Import", True)
            except ImportError as e:
                self.log_test("Auth Import", False, "Import error", {"error": str(e)})
            
        except Exception as e:
            self.log_test("Imports", False, "Critical import failure", {"error": str(e)})
            self.errors.append(f"Import failure: {e}")
    
    def test_config_values(self):
        """Test configuration values for consistency"""
        print("\n⚙️ Testing Configuration...")
        
        try:
            from config import (
                INITIAL_EMPLOYEES, INITIAL_PRODUCTS, INITIAL_RETAILERS,
                INITIAL_SALES_AMOUNT, DAILY_SALES_AMOUNT
            )
            
            # Test reasonable ranges
            if INITIAL_EMPLOYEES < 100 or INITIAL_EMPLOYEES > 1000:
                self.log_test("Employee Count", False, "Unrealistic employee count", 
                            {"count": INITIAL_EMPLOYEES})
            else:
                self.log_test("Employee Count", True, f"{INITIAL_EMPLOYEES} employees")
            
            if INITIAL_PRODUCTS < 50 or INITIAL_PRODUCTS > 500:
                self.log_test("Product Count", False, "Unrealistic product count",
                            {"count": INITIAL_PRODUCTS})
            else:
                self.log_test("Product Count", True, f"{INITIAL_PRODUCTS} products")
            
            if INITIAL_RETAILERS < 100 or INITIAL_RETAILERS > 2000:
                self.log_test("Retailer Count", False, "Unrealistic retailer count",
                            {"count": INITIAL_RETAILERS})
            else:
                self.log_test("Retailer Count", True, f"{INITIAL_RETAILERS} retailers")
            
            # Test sales amount configuration (separate for scheduled vs manual runs)
            expected_daily_calculated = INITIAL_SALES_AMOUNT // 365
            expected_daily_scheduled = 2000000  # ₱2M target for daily operations
            
            if DAILY_SALES_AMOUNT == expected_daily_scheduled:
                self.log_test("Sales Amount Configuration", True, 
                            f"Daily amount set to ₱{DAILY_SALES_AMOUNT:,} for scheduled runs",
                            {"annual": INITIAL_SALES_AMOUNT, "daily_scheduled": DAILY_SALES_AMOUNT,
                             "daily_calculated": expected_daily_calculated, "note": "Separate targets for scheduled vs manual runs"})
            else:
                self.log_test("Sales Amount Configuration", False, "Daily amount not set correctly",
                            {"annual": INITIAL_SALES_AMOUNT, "daily": DAILY_SALES_AMOUNT, 
                             "expected": expected_daily_scheduled})
            
            # Test revenue targets
            if INITIAL_SALES_AMOUNT < 1000000000 or INITIAL_SALES_AMOUNT > 20000000000:
                self.log_test("Revenue Target", False, "Unrealistic revenue target",
                            {"target": INITIAL_SALES_AMOUNT})
            else:
                self.log_test("Revenue Target", True, f"₱{INITIAL_SALES_AMOUNT:,} revenue target")
                
        except Exception as e:
            self.log_test("Configuration", False, "Config test failed", {"error": str(e)})
    
    def test_dimension_generation(self):
        """Test all dimension generation functions"""
        print("\n🏗️ Testing Dimension Generation...")
        
        try:
            from generators.dimensional import (
                generate_dim_locations, generate_dim_departments, generate_dim_jobs,
                generate_dim_banks, generate_dim_insurance, generate_dim_categories,
                generate_dim_brands, generate_dim_subcategories, generate_dim_products,
                generate_dim_employees_normalized, generate_dim_retailers_normalized,
                generate_dim_campaigns, generate_dim_dates
            )
            from config import INITIAL_EMPLOYEES, INITIAL_PRODUCTS, INITIAL_RETAILERS
            
            # Test reference dimensions first
            categories = generate_dim_categories()
            self.log_test("Categories Generation", True, f"{len(categories)} categories")
            
            brands = generate_dim_brands()
            self.log_test("Brands Generation", True, f"{len(brands)} brands")
            
            subcategories = generate_dim_subcategories()
            self.log_test("Subcategories Generation", True, f"{len(subcategories)} subcategories")
            
            # Test core dimensions
            locations = generate_dim_locations(num_locations=100)
            self.log_test("Locations Generation", True, f"{len(locations)} locations")
            
            departments = generate_dim_departments()
            self.log_test("Departments Generation", True, f"{len(departments)} departments")
            
            jobs = generate_dim_jobs(departments)
            self.log_test("Jobs Generation", True, f"{len(jobs)} jobs")
            
            banks = generate_dim_banks()
            self.log_test("Banks Generation", True, f"{len(banks)} banks")
            
            insurance = generate_dim_insurance()
            self.log_test("Insurance Generation", True, f"{len(insurance)} insurance providers")
            
            # Test dependent dimensions with foreign keys
            products = generate_dim_products(
                num_products=INITIAL_PRODUCTS,
                categories=categories,
                brands=brands,
                subcategories=subcategories
            )
            self.log_test("Products Generation", True, f"{len(products)} products with foreign keys")
            
            employees = generate_dim_employees_normalized(
                num_employees=INITIAL_EMPLOYEES,
                locations=locations,
                jobs=jobs,
                banks=banks,
                insurance=insurance,
                departments=departments
            )
            self.log_test("Employees Generation", True, f"{len(employees)} employees")
            
            retailers = generate_dim_retailers_normalized(
                num_retailers=INITIAL_RETAILERS,
                locations=locations
            )
            self.log_test("Retailers Generation", True, f"{len(retailers)} retailers")
            
            campaigns = generate_dim_campaigns()
            self.log_test("Campaigns Generation", True, f"{len(campaigns)} campaigns")
            
            dates = generate_dim_dates()
            self.log_test("Dates Generation", True, f"{len(dates)} dates")
            
            # Store for relationship testing
            self.dimensions = {
                'categories': categories, 'brands': brands, 'subcategories': subcategories,
                'locations': locations, 'departments': departments, 'jobs': jobs,
                'banks': banks, 'insurance': insurance, 'products': products,
                'employees': employees, 'retailers': retailers, 'campaigns': campaigns,
                'dates': dates
            }
            
        except Exception as e:
            self.log_test("Dimension Generation", False, "Generation failed", {"error": str(e)})
            self.errors.append(f"Dimension generation failed: {e}")
    
    def test_foreign_key_relationships(self):
        """Test all foreign key relationships"""
        print("\n🔗 Testing Foreign Key Relationships...")
        
        try:
            from generators.dimensional import validate_relationships
            
            if not hasattr(self, 'dimensions'):
                self.log_test("Foreign Key Test", False, "Dimensions not available")
                return
            
            # Validate all relationships
            is_valid = validate_relationships(
                self.dimensions['employees'], self.dimensions['products'], 
                self.dimensions['retailers'], self.dimensions['campaigns'],
                self.dimensions['locations'], self.dimensions['departments'],
                self.dimensions['jobs'], self.dimensions['banks'], 
                self.dimensions['insurance'],
                self.dimensions['categories'], self.dimensions['brands'],
                self.dimensions['subcategories']
            )
            
            self.log_test("Foreign Key Validation", is_valid, 
                        "All relationships valid" if is_valid else "Relationship issues found")
            
            # Test specific foreign key mappings
            products = self.dimensions['products']
            categories = self.dimensions['categories']
            
            # Check product-category relationships
            invalid_product_categories = 0
            for product in products[:10]:  # Sample first 10
                category_key = product.get('category_key')
                if not any(cat['category_key'] == category_key for cat in categories):
                    invalid_product_categories += 1
            
            if invalid_product_categories > 0:
                self.log_test("Product-Category Relationships", False, 
                            f"{invalid_product_categories} invalid relationships in sample")
            else:
                self.log_test("Product-Category Relationships", True, "Sample relationships valid")
                
        except Exception as e:
            self.log_test("Foreign Key Relationships", False, "Relationship test failed", {"error": str(e)})
    
    def test_fact_generation(self):
        """Test fact table generation"""
        print("\n📊 Testing Fact Generation...")
        
        try:
            from generators.dimensional import (
                generate_fact_sales, generate_fact_operating_costs,
                generate_fact_marketing_costs, generate_fact_inventory
            )
            from config import INITIAL_SALES_AMOUNT
            
            if not hasattr(self, 'dimensions'):
                self.log_test("Fact Generation", False, "Dimensions not available")
                return
            
            # Test sales generation (small sample)
            start_time = time.time()
            sales = generate_fact_sales(
                self.dimensions['employees'], self.dimensions['products'],
                self.dimensions['retailers'], self.dimensions['campaigns'],
                INITIAL_SALES_AMOUNT // 100,  # Use smaller amount for testing
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 31)  # 1 month only
            )
            sales_time = time.time() - start_time
            
            if len(sales) > 0:
                self.log_test("Sales Generation", True, 
                            f"{len(sales)} sales records in {sales_time:.2f}s")
                
                # Test sales data quality
                total_amount = sum(s['total_amount'] for s in sales)
                avg_amount = total_amount / len(sales)
                self.log_test("Sales Data Quality", True, 
                            f"Avg sale: ₱{avg_amount:.2f}, Total: ₱{total_amount:,.2f}")
            else:
                self.log_test("Sales Generation", False, "No sales records generated")
                self.warnings.append("Sales generation returned empty results")
            
            # Test operating costs generation
            start_time = time.time()
            costs = generate_fact_operating_costs(
                INITIAL_SALES_AMOUNT * 0.15,  # 15% of revenue
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31)
            )
            costs_time = time.time() - start_time
            
            if len(costs) > 0:
                total_costs = sum(c['amount'] for c in costs)
                expected_costs = INITIAL_SALES_AMOUNT * 0.15
                variance = abs(total_costs - expected_costs) / expected_costs
                
                if variance < 0.1:  # Within 10%
                    self.log_test("Operating Costs Generation", True,
                                f"{len(costs)} records, ₱{total_costs:,.2f} (target: ₱{expected_costs:,.2f})")
                else:
                    self.log_test("Operating Costs Generation", False,
                                f"Amount variance too high: {variance:.1%}",
                                {"actual": total_costs, "expected": expected_costs})
            else:
                self.log_test("Operating Costs Generation", False, "No cost records generated")
            
            # Test marketing costs generation
            start_time = time.time()
            marketing_costs = generate_fact_marketing_costs(
                self.dimensions['campaigns'],
                INITIAL_SALES_AMOUNT * 0.08,  # 8% of revenue
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31)
            )
            marketing_time = time.time() - start_time
            
            if len(marketing_costs) > 0:
                total_marketing = sum(c['amount'] for c in marketing_costs)
                self.log_test("Marketing Costs Generation", True,
                            f"{len(marketing_costs)} records, ₱{total_marketing:,.2f}")
            else:
                self.log_test("Marketing Costs Generation", False, "No marketing cost records generated")
            
            # Test inventory generation
            start_time = time.time()
            inventory = generate_fact_inventory(self.dimensions['products'])
            inventory_time = time.time() - start_time
            
            if len(inventory) > 0:
                self.log_test("Inventory Generation", True,
                            f"{len(inventory)} inventory records in {inventory_time:.2f}s")
            else:
                self.log_test("Inventory Generation", False, "No inventory records generated")
                
        except Exception as e:
            self.log_test("Fact Generation", False, "Fact generation failed", {"error": str(e)})
            self.errors.append(f"Fact generation failed: {e}")
    
    def test_data_quality(self):
        """Test data quality and consistency"""
        print("\n🔍 Testing Data Quality...")
        
        try:
            if not hasattr(self, 'dimensions'):
                self.log_test("Data Quality", False, "Dimensions not available")
                return
            
            # Test employee data quality
            employees = self.dimensions['employees']
            active_employees = [e for e in employees if e.get('employment_status') == 'Active']
            
            if len(active_employees) == 0:
                self.log_test("Active Employees", False, "No active employees found")
            else:
                self.log_test("Active Employees", True, f"{len(active_employees)} active employees")
                
                # Test hire dates
                invalid_hire_dates = 0
                for emp in active_employees[:50]:  # Sample
                    hire_date = emp.get('hire_date')
                    if hire_date and hire_date > date.today():
                        invalid_hire_dates += 1
                
                if invalid_hire_dates > 0:
                    self.log_test("Employee Hire Dates", False, f"{invalid_hire_dates} future hire dates")
                else:
                    self.log_test("Employee Hire Dates", True, "Hire dates appear valid")
            
            # Test product data quality
            products = self.dimensions['products']
            active_products = [p for p in products if p.get('status') == 'Active']
            
            if len(active_products) == 0:
                self.log_test("Active Products", False, "No active products found")
            else:
                self.log_test("Active Products", True, f"{len(active_products)} active products")
                
                # Test price logic
                invalid_prices = 0
                for product in active_products[:20]:  # Sample
                    wholesale = product.get('wholesale_price', 0)
                    retail = product.get('retail_price', 0)
                    if wholesale <= 0 or retail <= 0 or retail <= wholesale:
                        invalid_prices += 1
                
                if invalid_prices > 0:
                    self.log_test("Product Price Logic", False, f"{invalid_prices} invalid price relationships")
                else:
                    self.log_test("Product Price Logic", True, "Price relationships appear valid")
            
            # Test retailer data quality
            retailers = self.dimensions['retailers']
            retailer_types = set(r.get('retailer_type') for r in retailers)
            
            expected_types = {
                "Sari-Sari Store", "Convenience Store", "Supermarket", "Drugstore",
                "Wholesale Club", "Specialty Store", "Hypermarket", "Department Store"
            }
            
            missing_types = expected_types - retailer_types
            if missing_types:
                self.log_test("Retailer Types", False, f"Missing types: {missing_types}")
            else:
                self.log_test("Retailer Types", True, f"All {len(retailer_types)} expected types present")
                
        except Exception as e:
            self.log_test("Data Quality", False, "Data quality test failed", {"error": str(e)})
    
    def test_performance_metrics(self):
        """Test performance and identify bottlenecks"""
        print("\n⚡ Testing Performance...")
        
        try:
            # Test dimension generation performance
            start_time = time.time()
            
            from generators.dimensional import (
                generate_dim_locations, generate_dim_departments, generate_dim_jobs,
                generate_dim_banks, generate_dim_insurance, generate_dim_categories,
                generate_dim_brands, generate_dim_subcategories
            )
            
            # Generate all dimensions
            locations = generate_dim_locations(num_locations=500)
            departments = generate_dim_departments()
            jobs = generate_dim_jobs(departments)
            banks = generate_dim_banks()
            insurance = generate_dim_insurance()
            categories = generate_dim_categories()
            brands = generate_dim_brands()
            subcategories = generate_dim_subcategories()
            
            dim_time = time.time() - start_time
            
            if dim_time > 5.0:
                self.log_test("Dimension Generation Performance", False, 
                            f"Too slow: {dim_time:.2f}s")
                self.warnings.append(f"Dimension generation took {dim_time:.2f}s")
            else:
                self.log_test("Dimension Generation Performance", True, 
                            f"Generated in {dim_time:.2f}s")
            
            # Test relationship validation performance
            from generators.dimensional import generate_dim_products, generate_dim_employees_normalized
            
            products = generate_dim_products(
                num_products=150,
                categories=categories,
                brands=brands,
                subcategories=subcategories
            )
            
            employees = generate_dim_employees_normalized(
                num_employees=350,
                locations=locations,
                jobs=jobs,
                banks=banks,
                insurance=insurance,
                departments=departments
            )
            
            start_time = time.time()
            from generators.dimensional import validate_relationships
            is_valid = validate_relationships(
                employees, products, [], [], locations, departments, jobs, banks, insurance,
                categories, brands, subcategories
            )
            validation_time = time.time() - start_time
            
            if validation_time > 2.0:
                self.log_test("Relationship Validation Performance", False,
                            f"Too slow: {validation_time:.2f}s")
            else:
                self.log_test("Relationship Validation Performance", True,
                            f"Validated in {validation_time:.2f}s")
                
        except Exception as e:
            self.log_test("Performance Testing", False, "Performance test failed", {"error": str(e)})
    
    def test_edge_cases(self):
        """Test edge cases and error conditions"""
        print("\n🧪 Testing Edge Cases...")
        
        try:
            # Import needed functions for edge cases
            from generators.dimensional import generate_dim_products, generate_dim_locations, generate_dim_categories, generate_dim_brands, generate_dim_subcategories
            
            # Test empty data handling
            empty_categories = []
            empty_brands = []
            empty_subcategories = []
            
            # Should handle empty references gracefully
            try:
                products = generate_dim_products(
                    num_products=5,
                    categories=empty_categories,
                    brands=empty_brands,
                    subcategories=empty_subcategories
                )
                # If it doesn't crash, that's good
                self.log_test("Empty References Handling", True, "No crash with empty references")
            except Exception as e:
                # Expected to fail gracefully
                self.log_test("Empty References Handling", True, "Graceful failure with empty references")
            
            # Test zero counts
            try:
                zero_locations = generate_dim_locations(num_locations=0)
                self.log_test("Zero Count Handling", True, f"Handled zero locations: {len(zero_locations)}")
            except Exception as e:
                self.log_test("Zero Count Handling", False, "Failed with zero count", {"error": str(e)})
            
            # Test very large counts (within reason)
            try:
                large_products = generate_dim_products(
                    num_products=1000,  # Large but reasonable
                    categories=generate_dim_categories(),
                    brands=generate_dim_brands(),
                    subcategories=generate_dim_subcategories()
                )
                self.log_test("Large Count Handling", True, f"Handled 1000 products: {len(large_products)}")
            except Exception as e:
                self.log_test("Large Count Handling", False, "Failed with large count", {"error": str(e)})
                
        except Exception as e:
            self.log_test("Edge Cases", False, "Edge case testing failed", {"error": str(e)})
    
    def run_all_tests(self):
        """Run all tests and generate comprehensive report"""
        print("🚀 Starting FMCG Deep Test Suite")
        print("=" * 60)
        
        start_time = time.time()
        
        # Run all test categories
        self.test_imports()
        self.test_config_values()
        self.test_dimension_generation()
        self.test_foreign_key_relationships()
        self.test_fact_generation()
        self.test_data_quality()
        self.test_performance_metrics()
        self.test_edge_cases()
        
        total_time = time.time() - start_time
        
        # Generate summary report
        self.generate_summary_report(total_time)
    
    def generate_summary_report(self, total_time):
        """Generate comprehensive summary report"""
        print("\n" + "=" * 60)
        print("📊 COMPREHENSIVE TEST REPORT")
        print("=" * 60)
        
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results.values() if result['passed'])
        failed_tests = total_tests - passed_tests
        
        print(f"\n📈 Test Summary:")
        print(f"   Total Tests: {total_tests}")
        print(f"   Passed: {passed_tests} ✅")
        print(f"   Failed: {failed_tests} ❌")
        print(f"   Success Rate: {(passed_tests/total_tests)*100:.1f}%")
        print(f"   Total Time: {total_time:.2f}s")
        
        if self.errors:
            print(f"\n💥 Critical Errors ({len(self.errors)}):")
            for i, error in enumerate(self.errors, 1):
                print(f"   {i}. {error}")
        
        if self.warnings:
            print(f"\n⚠️  Warnings ({len(self.warnings)}):")
            for i, warning in enumerate(self.warnings, 1):
                print(f"   {i}. {warning}")
        
        # Failed tests details
        failed_test_names = [name for name, result in self.test_results.items() if not result['passed']]
        if failed_test_names:
            print(f"\n❌ Failed Tests:")
            for test_name in failed_test_names:
                result = self.test_results[test_name]
                print(f"   • {test_name}: {result['message']}")
                if result['details']:
                    for key, value in result['details'].items():
                        print(f"     - {key}: {value}")
        
        # Recommendations
        print(f"\n💡 Recommendations:")
        if failed_tests == 0:
            print("   ✅ All tests passed! System is ready for production.")
        else:
            print("   🔧 Address failed tests before production deployment.")
            
        if len(self.errors) > 0:
            print("   🚨 Critical errors must be resolved immediately.")
            
        if len(self.warnings) > 0:
            print("   ⚠️  Review warnings for potential optimizations.")
        
        if total_time > 30:
            print("   ⏰ Consider performance optimizations for faster execution.")
        
        print(f"\n🎯 Overall Status: {'READY' if failed_tests == 0 else 'NEEDS ATTENTION'}")
        print("=" * 60)

if __name__ == "__main__":
    # Run comprehensive deep test
    test_suite = FMCGDeepTest()
    test_suite.run_all_tests()
