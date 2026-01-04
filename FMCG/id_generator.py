"""
Centralized ID Generation System for FMCG Data Analytics
Ensures unique ID generation across all tables with BigQuery and GitHub Actions compatibility
"""

import hashlib
import time
import random
import os
from datetime import datetime
from typing import Dict, Any, Optional

class IDGenerator:
    """Centralized ID generator with deterministic behavior for CI/CD environments"""
    
    def __init__(self, seed: Optional[int] = None):
        """
        Initialize ID generator with optional seed for reproducible results
        """
        if seed is None:
            # Use environment-based seed for GitHub Actions consistency
            base_seed = int(time.time() * 1000) % (2**31)
            # Add GitHub Actions run ID if available
            github_run_id = os.environ.get('GITHUB_RUN_ID', '')
            if github_run_id:
                # Hash the run ID and incorporate it into seed
                run_hash = int(hashlib.md5(github_run_id.encode()).hexdigest()[:8], 16)
                base_seed ^= run_hash
                # Try to parse numeric part of run ID, fallback to hash if not numeric
                try:
                    numeric_part = int(github_run_id[-8:] if len(github_run_id) >= 8 else github_run_id, 16) % (2**16)
                except ValueError:
                    # If run ID contains non-numeric characters, use hash of the full ID
                    numeric_part = int(hashlib.md5(github_run_id.encode()).hexdigest()[8:16], 16) % (2**16)
                base_seed ^= numeric_part
            # Add process ID for uniqueness
            base_seed ^= os.getpid() % (2**16)
            seed = base_seed
        
        self.random = random.Random(seed)
        self.counters: Dict[str, int] = {}
        self.global_counter = 1
        self.seed = seed  # Store seed for debugging
        
    def generate_key(self, table_name: str, prefix: str = "", suffix: str = "") -> int:
        """
        Generate a unique integer key for a table
        Uses table-specific counters to ensure uniqueness
        """
        if table_name not in self.counters:
            # Initialize with table-specific offset based on hash and seed
            table_hash = int(hashlib.md5(f"{table_name}_{self.seed}".encode()).hexdigest()[:8], 16)
            self.counters[table_name] = (table_hash % 10000) + 1
        
        key = self.counters[table_name]
        self.counters[table_name] += 1
        return key
    
    def generate_id(self, table_name: str, prefix: str = "", numeric_id: Optional[int] = None) -> str:
        """
        Generate a formatted string ID (e.g., EMP000001, R0001, C001)
        """
        if numeric_id is None:
            numeric_id = self.generate_key(table_name)
        
        # Determine padding based on table type
        padding_map = {
            'dim_employees': 6,
            'dim_retailers': 4,
            'dim_campaigns': 3,
            'dim_products': 4,
            'dim_locations': 4,
            'dim_jobs': 3,
            'dim_departments': 2,
            'dim_banks': 2,
            'dim_insurance': 2,
            'fact_sales': 8,
            'fact_employees': 6,
            'fact_employee_wages': 8,
            'fact_operating_costs': 6,
            'fact_marketing_costs': 6,
            'fact_inventory': 6,
            'dim_dates': 8,
        }
        
        padding = padding_map.get(table_name, 6)
        
        # Format prefix
        if not prefix:
            prefix_map = {
                'dim_employees': 'EMP',
                'dim_retailers': 'R',
                'dim_campaigns': 'C',
                'dim_products': 'P',
                'dim_locations': 'LOC',
                'dim_jobs': 'JOB',
                'dim_departments': 'DEPT',
                'dim_banks': 'BANK',
                'dim_insurance': 'INS',
                'fact_sales': 'S',
                'fact_employees': 'FE',
                'fact_employee_wages': 'FW',
                'fact_operating_costs': 'FC',
                'fact_marketing_costs': 'FM',
                'fact_inventory': 'FI',
                'dim_dates': 'D',
            }
            prefix = prefix_map.get(table_name, 'ID')
        
        return f"{prefix}{numeric_id:0{padding}d}"
    
    def generate_government_id(self, id_type: str) -> str:
        """
        Generate government ID numbers with proper format
        """
        if id_type == 'tin':
            # TIN: 9 digits
            return f"{self.random.randint(100000000, 999999999)}"
        elif id_type == 'sss':
            # SSS: 10 digits
            return f"{self.random.randint(1000000000, 9999999999)}"
        elif id_type == 'philhealth':
            # PhilHealth: 12 digits
            return f"{self.random.randint(100000000000, 999999999999)}"
        elif id_type == 'pagibig':
            # Pag-IBIG: 10 digits
            return f"{self.random.randint(1000000000, 9999999999)}"
        else:
            # Default: 8 digits
            return f"{self.random.randint(10000000, 99999999)}"
    
    def generate_unique_key(self, table_name: str, *components: Any) -> str:
        """
        Generate a deterministic unique key based on components
        Useful for composite keys or location-based keys
        """
        # Create deterministic hash from components
        key_string = "|".join(str(comp) for comp in components)
        hash_obj = hashlib.md5(key_string.encode())
        
        # Convert to numeric and ensure positive
        numeric_hash = int(hash_obj.hexdigest()[:8], 16)
        return str(abs(numeric_hash))
    
    def reset_counter(self, table_name: str, start_value: int = 1):
        """
        Reset counter for a specific table (useful for testing)
        """
        self.counters[table_name] = start_value
    
    def get_next_id(self, table_name: str) -> int:
        """
        Get the next ID that will be generated for a table
        """
        return self.counters.get(table_name, 1)

# Global instance for use across the application
_id_generator = None

def get_id_generator(seed: Optional[int] = None) -> IDGenerator:
    """
    Get the global ID generator instance
    """
    global _id_generator
    if _id_generator is None:
        _id_generator = IDGenerator(seed)
    return _id_generator

def reset_id_generator(seed: Optional[int] = None):
    """
    Reset the global ID generator (useful for testing)
    """
    global _id_generator
    _id_generator = IDGenerator(seed)

# Convenience functions for backward compatibility
def generate_key(table_name: str, prefix: str = "", suffix: str = "") -> int:
    """Generate a unique integer key"""
    return get_id_generator().generate_key(table_name, prefix, suffix)

def generate_id(table_name: str, prefix: str = "", numeric_id: Optional[int] = None) -> str:
    """Generate a formatted string ID"""
    return get_id_generator().generate_id(table_name, prefix, numeric_id)

def generate_government_id(id_type: str) -> str:
    """Generate government ID numbers"""
    return get_id_generator().generate_government_id(id_type)

def generate_unique_key(table_name: str, *components: Any) -> str:
    """Generate deterministic unique key from components"""
    return get_id_generator().generate_unique_key(table_name, *components)
