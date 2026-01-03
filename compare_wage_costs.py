#!/usr/bin/env python3
"""
Compare different wage cost calculations
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'FMCG'))

from FMCG.generators.dimensional import *
from FMCG.config import *
from datetime import date, timedelta

def compare_wage_costs():
    """Compare different wage cost calculations"""
    print("=" * 80)
    print("COMPARING WAGE COST CALCULATIONS")
    print("=" * 80)
    
    # Generate test data
    locations = generate_dim_locations(10)
    departments = generate_dim_departments()
    jobs = generate_dim_jobs(departments)
    banks = generate_dim_banks()
    insurance = generate_dim_insurance()
    
    employees = generate_dim_employees_normalized(
        20, locations, jobs, banks, insurance, departments
    )
    
    wages = generate_fact_employee_wages(employees, jobs)
    
    print(f"\n📊 DATA SUMMARY:")
    print(f"  Employees: {len(employees)}")
    print(f"  Active Employees: {len([e for e in employees if e['employment_status'] == 'Active'])}")
    print(f"  Total Wage Records: {len(wages)}")
    
    # Method 1: Current Annual Wage Cost
    latest_salaries = {}
    for wage in wages:
        emp_key = wage['employee_key']
        if emp_key not in latest_salaries or wage['effective_date'] > latest_salaries[emp_key]['effective_date']:
            latest_salaries[emp_key] = wage
    
    current_annual_cost = sum(w['annual_salary'] for w in latest_salaries.values())
    
    # Method 2: Total Historical Wage Cost
    total_historical_cost = sum(w['monthly_salary'] for w in wages)
    
    # Method 3: Annual Cost by Year
    annual_costs = {}
    for wage in wages:
        year = wage['effective_date'].year
        if year not in annual_costs:
            annual_costs[year] = 0
        annual_costs[year] += wage['monthly_salary']
    
    print(f"\n💰 WAGE COST COMPARISON:")
    print(f"  Current Annual Cost: ₱{current_annual_cost:,.0f}")
    print(f"  Total Historical Cost: ₱{total_historical_cost:,.0f}")
    print(f"  Ratio: {total_historical_cost/current_annual_cost:.1f}x")
    
    print(f"\n📅 ANNUAL COSTS BY YEAR:")
    for year in sorted(annual_costs.keys()):
        print(f"  {year}: ₱{annual_costs[year]:,.0f}")
    
    # Show what each represents
    print(f"\n📖 WHAT EACH MEANS:")
    print(f"  Current Annual Cost: What we'll pay this year")
    print(f"  Total Historical Cost: What we've paid since 2015")
    print(f"  Annual Costs: How much we paid each specific year")
    
    # Business context
    print(f"\n🏢 BUSINESS CONTEXT:")
    print(f"  Budget Planning: Use Current Annual Cost (₱{current_annual_cost:,.0f})")
    print(f"  Financial Reporting: Use Annual Costs by Year")
    print(f"  Total Investment: Use Historical Cost (₱{total_historical_cost:,.0f})")

if __name__ == "__main__":
    compare_wage_costs()
