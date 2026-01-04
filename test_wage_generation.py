#!/usr/bin/env python3
"""
Test script to verify wage generation fixes for 2015-present coverage
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'FMCG'))

from datetime import date
from FMCG.generators.dimensional import generate_fact_employee_wages

def test_wage_generation_2015():
    """Test that wage generation starts from 2015 for all employees"""
    print("Testing wage generation from 2015...")
    
    # Create test employees with different hire dates
    employees = [
        {
            "employee_key": 1,
            "job_key": 1,
            "hire_date": date(2018, 6, 15),  # Hired after 2015
            "employment_status": "Active",
            "termination_date": None
        },
        {
            "employee_key": 2,
            "job_key": 2,
            "hire_date": date(2014, 3, 1),   # Hired before 2015
            "employment_status": "Active",
            "termination_date": None
        },
        {
            "employee_key": 3,
            "job_key": 3,
            "hire_date": date(2020, 1, 1),   # Hired recently
            "employment_status": "Terminated",
            "termination_date": date(2022, 12, 31)
        }
    ]
    
    # Create test jobs
    jobs = [
        {
            "job_key": 1,
            "job_title": "Sales Representative",
            "job_level": "Junior",
            "base_salary_min": 25000,
            "base_salary_max": 40000,
            "work_type": "Full-time"
        },
        {
            "job_key": 2,
            "job_title": "Senior Manager",
            "job_level": "Manager",
            "base_salary_min": 70000,
            "base_salary_max": 90000,
            "work_type": "Full-time"
        },
        {
            "job_key": 3,
            "job_title": "Intern",
            "job_level": "Entry",
            "base_salary_min": 15000,
            "base_salary_max": 25000,
            "work_type": "Intern"
        }
    ]
    
    # Generate wages with 2015 start date
    wages = generate_fact_employee_wages(
        employees, jobs,
        start_date=date(2015, 1, 1),
        end_date=date(2024, 12, 31)
    )
    
    print(f"Generated {len(wages)} wage records")
    
    # Check that we have data starting from 2015 for eligible employees
    employee_1_wages = [w for w in wages if w["employee_key"] == 1]
    employee_2_wages = [w for w in wages if w["employee_key"] == 2]
    employee_3_wages = [w for w in wages if w["employee_key"] == 3]
    
    print(f"Employee 1 (hired 2018): {len(employee_1_wages)} wage records")
    print(f"Employee 2 (hired 2014): {len(employee_2_wages)} wage records")
    print(f"Employee 3 (hired 2020, terminated 2022): {len(employee_3_wages)} wage records")
    
    # Check employee 1 (hired 2018) - should have records from 2018 to 2024
    if employee_1_wages:
        earliest_date = min(w["effective_date"] for w in employee_1_wages)
        latest_date = max(w["effective_date"] for w in employee_1_wages)
        print(f"Employee 1 wage period: {earliest_date} to {latest_date}")
        assert earliest_date.year == 2018, f"Expected 2018 start, got {earliest_date.year}"
        assert latest_date.year == 2024, f"Expected 2024 end, got {latest_date.year}"
    
    # Check employee 2 (hired 2014) - should have records from 2015 to 2024
    if employee_2_wages:
        earliest_date = min(w["effective_date"] for w in employee_2_wages)
        latest_date = max(w["effective_date"] for w in employee_2_wages)
        print(f"Employee 2 wage period: {earliest_date} to {latest_date}")
        assert earliest_date.year == 2015, f"Expected 2015 start, got {earliest_date.year}"
        assert latest_date.year == 2024, f"Expected 2024 end, got {latest_date.year}"
    
    # Check employee 3 (hired 2020, terminated 2022) - should have records from 2020 to 2022
    if employee_3_wages:
        earliest_date = min(w["effective_date"] for w in employee_3_wages)
        latest_date = max(w["effective_date"] for w in employee_3_wages)
        print(f"Employee 3 wage period: {earliest_date} to {latest_date}")
        assert earliest_date.year == 2020, f"Expected 2020 start, got {earliest_date.year}"
        assert latest_date.year == 2022, f"Expected 2022 end, got {latest_date.year}"
    
    # Verify all required fields are present
    if wages:
        sample_record = wages[0]
        required_fields = [
            'wage_key', 'employee_key', 'effective_date', 'job_title',
            'job_level', 'department', 'monthly_salary', 'annual_salary',
            'currency', 'years_of_service', 'salary_grade'
        ]
        
        for field in required_fields:
            assert field in sample_record, f"Missing field: {field}"
        
        print("✓ All required fields present")
    
    print("✓ Wage generation test passed")

if __name__ == "__main__":
    try:
        test_wage_generation_2015()
        print("\n✅ All wage generation tests passed!")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
