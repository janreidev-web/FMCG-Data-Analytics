"""
Centralized ID Generation Module for FMCG Analytics
Provides unique ID generation functions with proper state management
"""

import hashlib
import time
import uuid
from typing import Dict, Any, Optional

# Global ID generation state to ensure uniqueness across all runs
ID_GENERATOR_STATE: Dict[str, Any] = {
    'sequence_counters': {},
    'session_id': str(uuid.uuid4())[:8],
    'timestamp_base': int(time.time() * 1000)
}

def generate_unique_id(entity_type: str, use_timestamp: bool = True) -> int:
    """
    Generate truly unique IDs using entity type, session, timestamp, and sequence
    
    Args:
        entity_type: Type of entity (e.g., 'employee', 'product', 'retailer')
        use_timestamp: Whether to use timestamp-based generation (default: True)
    
    Returns:
        Unique integer ID
    """
    # Get or initialize sequence counter for this entity type
    if entity_type not in ID_GENERATOR_STATE['sequence_counters']:
        ID_GENERATOR_STATE['sequence_counters'][entity_type] = 0
    
    # Increment sequence counter
    ID_GENERATOR_STATE['sequence_counters'][entity_type] += 1
    sequence = ID_GENERATOR_STATE['sequence_counters'][entity_type]
    
    if use_timestamp:
        # Use timestamp + session + entity type + sequence for uniqueness
        timestamp_ms = ID_GENERATOR_STATE['timestamp_base'] + sequence
        unique_str = f"{ID_GENERATOR_STATE['session_id']}{entity_type}{timestamp_ms}{sequence}"
        # Create hash to ensure reasonable length
        unique_hash = hashlib.md5(unique_str.encode()).hexdigest()[:12]
        unique_id = int(unique_hash, 16)
        
        # Ensure it fits in 19 digits (BigQuery INTEGER limit: 2^63-1 = 9,223,372,036,854,775,807)
        max_safe_int = 9223372036854775807  # Max 64-bit signed integer
        if unique_id > max_safe_int:
            # If too large, take modulo to fit within range
            unique_id = unique_id % max_safe_int
        
        return unique_id
    else:
        # For cases where we want more readable IDs
        return sequence

def generate_readable_id(prefix: str, entity_type: str, padding: int = 4) -> str:
    """
    Generate readable but unique IDs with prefix
    
    Args:
        prefix: Prefix for the ID (e.g., 'EMP', 'P', 'R')
        entity_type: Type of entity for sequence tracking
        padding: Number of digits for zero-padding
    
    Returns:
        Formatted readable ID string
    """
    unique_num = generate_unique_id(entity_type, use_timestamp=False)
    return f"{prefix}{unique_num:0{padding}d}"

def generate_unique_sale_key(sale_date: str, product_key: int, employee_key: int, 
                            retailer_key: int, sequence_num: int, timestamp_ms: Optional[int] = None) -> int:
    """
    Generate unique composite key for sales transactions
    
    Args:
        sale_date: Date of the sale
        product_key: Product identifier
        employee_key: Employee identifier
        retailer_key: Retailer identifier
        sequence_num: Sequence number for the day
        timestamp_ms: Optional timestamp in milliseconds
    
    Returns:
        Unique composite sale key as integer
    """
    if timestamp_ms is None:
        timestamp_ms = int(time.time() * 1000)
    
    # Create composite key using all identifying information
    key_components = f"{sale_date}_{product_key}_{employee_key}_{retailer_key}_{sequence_num}_{timestamp_ms}"
    # Hash to create fixed-length key
    key_hash = hashlib.md5(key_components.encode()).hexdigest()[:16]
    # Convert to integer to match schema
    unique_id = int(key_hash, 16)
    
    # Ensure it fits in 19 digits (BigQuery INTEGER limit: 2^63-1 = 9,223,372,036,854,775,807)
    max_safe_int = 9223372036854775807  # Max 64-bit signed integer
    if unique_id > max_safe_int:
        # If too large, take modulo to fit within range
        unique_id = unique_id % max_safe_int
    
    return unique_id

def reset_id_counters(entity_type: Optional[str] = None) -> None:
    """
    Reset ID generation counters. Use with caution!
    
    Args:
        entity_type: Specific entity type to reset, or None to reset all
    """
    if entity_type:
        ID_GENERATOR_STATE['sequence_counters'].pop(entity_type, None)
    else:
        ID_GENERATOR_STATE['sequence_counters'].clear()

def get_id_generation_stats() -> Dict[str, Any]:
    """
    Get statistics about ID generation state
    
    Returns:
        Dictionary with generation statistics
    """
    return {
        'session_id': ID_GENERATOR_STATE['session_id'],
        'timestamp_base': ID_GENERATOR_STATE['timestamp_base'],
        'entity_counters': ID_GENERATOR_STATE['sequence_counters'].copy()
    }
