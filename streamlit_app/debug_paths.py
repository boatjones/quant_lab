"""
Debug script to test path resolution in quant_lab structure
Place this in quant_lab/streamlit_app/ and run it
"""

from pathlib import Path
import sys

print("="*70)
print("Path Debug Information")
print("="*70)

# Current file location
print(f"\n__file__ = {__file__}")

# Current working directory
print(f"Current working directory = {Path.cwd()}")

# Calculate project root
print(f"\nPath(__file__) = {Path(__file__)}")
print(f"Path(__file__).resolve() = {Path(__file__).resolve()}")
print(f"Path(__file__).parent = {Path(__file__).parent}")
print(f"Path(__file__).parents[0] = {Path(__file__).parents[0]}")
print(f"Path(__file__).parents[1] = {Path(__file__).parents[1]}")

# Test both methods
project_root_method1 = Path(__file__).parents[1]
project_root_method2 = Path(__file__).parent.parent

print(f"\nMethod 1 (parents[1]): {project_root_method1}")
print(f"Method 2 (parent.parent): {project_root_method2}")

# Check if they're the same
print(f"\nMethods match: {project_root_method1 == project_root_method2}")

# Check for util directory
util_path = project_root_method1 / 'util'
print(f"\nUtil directory path: {util_path}")
print(f"Util directory exists: {util_path.exists()}")

# Check for to_postgres.py
to_postgres_path = util_path / 'to_postgres.py'
print(f"to_postgres.py path: {to_postgres_path}")
print(f"to_postgres.py exists: {to_postgres_path.exists()}")

# Check sys.path
print(f"\nCurrent sys.path:")
for i, path in enumerate(sys.path[:5]):
    print(f"  {i}: {path}")

# Try adding to sys.path and importing
print("\n" + "="*70)
print("Testing Import")
print("="*70)

try:
    # Add to path
    sys.path.insert(0, str(project_root_method1))
    print(f"\nAdded to sys.path: {project_root_method1}")
    
    # Try import
    print("Attempting import...")
    from util.to_postgres import PgHook
    print("✓ SUCCESS: PgHook imported successfully!")
    print(f"PgHook class: {PgHook}")
    
except ImportError as e:
    print(f"✗ FAILED: Import error: {e}")
    print("\nDebugging steps:")
    print("1. Check if util/__init__.py exists")
    print("2. Check if util directory is in project_root")
    print("3. Check if to_postgres.py is in util directory")
    
    # More detailed debugging
    print("\nDirectory contents:")
    print(f"\nContents of {project_root_method1}:")
    if project_root_method1.exists():
        for item in sorted(project_root_method1.iterdir()):
            print(f"  - {item.name}")
    
    if util_path.exists():
        print(f"\nContents of {util_path}:")
        for item in sorted(util_path.iterdir()):
            print(f"  - {item.name}")
    
except Exception as e:
    print(f"✗ FAILED: Unexpected error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*70)
print("Debug Complete")
print("="*70)
