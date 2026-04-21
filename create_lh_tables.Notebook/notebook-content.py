# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1a66d26a-a3e1-47d6-8e75-466457c017ae",
# META       "default_lakehouse_name": "bronzelh",
# META       "default_lakehouse_workspace_id": "5369a871-dda0-48d9-b3a8-3265715b6190",
# META       "known_lakehouses": [
# META         {
# META           "id": "1a66d26a-a3e1-47d6-8e75-466457c017ae"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Lakehouse Table Creation from SQL Scripts
# 
# This notebook reads table creation scripts from files stored in the `builtin` folder and executes them to create tables in the default Lakehouse.
# 
# ## Features:
# - **Flexible Execution**: Run all scripts or specific scripts by name
# - **Script Management**: Automatically discovers all `.sql` files in the `builtin/scripts` folder
# - **Error Handling**: Captures and reports execution errors for each script
# - **Progress Tracking**: Shows which scripts are being executed and their status
# 
# ## Setup Instructions:
# 1. Create a folder named `builtin/scripts` in the notebook resources
# 2. Place your table creation SQL scripts (`.sql` files) in that folder
# 3. Each SQL file should contain one or more CREATE TABLE statements
# 4. Run the cells below to execute the scripts

# CELL ********************

# Import required libraries
import os
import glob
from pathlib import Path
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get the notebook resource path
resource_path = notebookutils.nbResPath
scripts_folder = f"{resource_path}/builtin/scripts"

print(f"Notebook Resource Path: {resource_path}")
print(f"SQL Scripts Folder: {scripts_folder}")

# Verify the folder exists
if os.path.exists(scripts_folder):
    print(f"✓ Scripts folder exists")
else:
    print(f"⚠ Scripts folder does not exist. Please create it and add your .sql files.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def list_sql_scripts(scripts_path):
    """
    List all SQL script files in the specified folder.
    
    Args:
        scripts_path (str): Path to the folder containing SQL scripts
        
    Returns:
        list: List of SQL script file paths
    """
    if not os.path.exists(scripts_path):
        print(f"Error: Scripts folder '{scripts_path}' does not exist.")
        return []
    
    # Find all .sql files
    sql_files = glob.glob(f"{scripts_path}/*.sql")
    
    if not sql_files:
        print(f"No .sql files found in '{scripts_path}'")
        return []
    
    print(f"Found {len(sql_files)} SQL script(s):")
    for idx, file_path in enumerate(sorted(sql_files), 1):
        filename = os.path.basename(file_path)
        print(f"  {idx}. {filename}")
    
    return sorted(sql_files)

# List available scripts
available_scripts = list_sql_scripts(scripts_folder)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def execute_sql_script(script_path):
    """
    Execute a SQL script file in the Spark session.
    
    Args:
        script_path (str): Path to the SQL script file
        
    Returns:
        tuple: (success: bool, message: str)
    """
    filename = os.path.basename(script_path)
    
    try:
        # Read the SQL script content
        with open(script_path, 'r') as f:
            sql_content = f.read()
        
        if not sql_content.strip():
            return False, f"Script '{filename}' is empty"
        
        # Split the content by semicolons to handle multiple statements
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        print(f"  Executing {len(statements)} statement(s) from '{filename}'...")
        
        # Execute each SQL statement
        for idx, statement in enumerate(statements, 1):
            if statement:
                try:
                    spark.sql(statement)
                    print(f"    ✓ Statement {idx} executed successfully")
                except Exception as stmt_error:
                    return False, f"Error in statement {idx}: {str(stmt_error)}"
        
        return True, f"All statements executed successfully"
        
    except FileNotFoundError:
        return False, f"Script file '{filename}' not found"
    except Exception as e:
        return False, f"Error reading/executing script: {str(e)}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_tables(script_names=None, scripts_path=None):
    """
    Execute table creation scripts.
    
    Args:
        script_names (list, optional): List of specific script filenames to execute.
                                       If None, executes all scripts in the folder.
        scripts_path (str, optional): Path to the scripts folder. 
                                      Defaults to builtin/scripts.
        
    Returns:
        dict: Execution results summary
    """
    if scripts_path is None:
        scripts_path = scripts_folder
    
    # Get list of scripts to execute
    all_scripts = list_sql_scripts(scripts_path)
    
    if not all_scripts:
        return {"status": "error", "message": "No scripts found"}
    
    # Filter scripts if specific names provided
    if script_names:
        scripts_to_run = []
        for name in script_names:
            # Handle both with and without .sql extension
            if not name.endswith('.sql'):
                name = f"{name}.sql"
            
            matching_scripts = [s for s in all_scripts if os.path.basename(s) == name]
            if matching_scripts:
                scripts_to_run.extend(matching_scripts)
            else:
                print(f"⚠ Warning: Script '{name}' not found")
        
        if not scripts_to_run:
            return {"status": "error", "message": "No matching scripts found"}
    else:
        scripts_to_run = all_scripts
    
    # Execute scripts
    print(f"\n{'='*60}")
    print(f"Starting table creation at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    results = {
        "total": len(scripts_to_run),
        "success": 0,
        "failed": 0,
        "details": []
    }
    
    for idx, script_path in enumerate(scripts_to_run, 1):
        filename = os.path.basename(script_path)
        print(f"[{idx}/{len(scripts_to_run)}] Processing: {filename}")
        
        success, message = execute_sql_script(script_path)
        
        result_entry = {
            "script": filename,
            "success": success,
            "message": message
        }
        results["details"].append(result_entry)
        
        if success:
            results["success"] += 1
            print(f"  ✓ SUCCESS: {message}\n")
        else:
            results["failed"] += 1
            print(f"  ✗ FAILED: {message}\n")
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"EXECUTION SUMMARY")
    print(f"{'='*60}")
    print(f"Total Scripts: {results['total']}")
    print(f"Successful: {results['success']}")
    print(f"Failed: {results['failed']}")
    print(f"{'='*60}\n")
    
    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## Usage Examples
# 
# Below are examples of how to use the `create_tables()` function:
# 
# ### Option 1: Run ALL scripts
# Execute all `.sql` files in the `builtin/scripts` folder:
# ```python
# results = create_tables()
# ```
# 
# ### Option 2: Run SPECIFIC scripts
# Execute only specified script files:
# ```python
# # By filename (with or without .sql extension)
# results = create_tables(script_names=['create_customers_table', 'create_orders_table.sql'])
# ```
# 
# ### Option 3: Custom scripts path
# Execute scripts from a different folder:
# ```python
# results = create_tables(scripts_path=f"{resource_path}/builtin/custom_scripts")
# ```

# CELL ********************

# OPTION 1: Execute ALL SQL scripts in the builtin/scripts folder
# Uncomment the line below to run all scripts

results = create_tables()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# OPTION 2: Execute SPECIFIC scripts by name
# Specify the script filenames (with or without .sql extension)
# Uncomment and modify the lines below to run specific scripts

# script_list = ['create_table1', 'create_table2.sql']  # Add your script names here
# results = create_tables(script_names=script_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify created tables
# List all tables in the default lakehouse to confirm creation

tables = spark.catalog.listTables()

print(f"Total tables in lakehouse: {len(tables)}\n")
print("Table List:")
print("-" * 60)
for table in tables:
    print(f"  • {table.name} (Database: {table.database})")
print("-" * 60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ---
# ## Example SQL Script Files
# 
# Create `.sql` files in the `builtin/scripts` folder with your table creation statements. Here are some examples:
# 
# **Example 1: `create_customers_table.sql`**
# ```sql
# CREATE TABLE IF NOT EXISTS customers (
#     customer_id INT,
#     customer_name STRING,
#     email STRING,
#     created_date TIMESTAMP
# ) USING DELTA;
# ```
# 
# **Example 2: `create_orders_table.sql`**
# ```sql
# CREATE TABLE IF NOT EXISTS orders (
#     order_id INT,
#     customer_id INT,
#     order_date TIMESTAMP,
#     total_amount DECIMAL(10,2),
#     status STRING
# ) USING DELTA;
# 
# CREATE TABLE IF NOT EXISTS order_items (
#     item_id INT,
#     order_id INT,
#     product_id INT,
#     quantity INT,
#     price DECIMAL(10,2)
# ) USING DELTA;
# ```
# 
# **Tips:**
# - Use `IF NOT EXISTS` to avoid errors if tables already exist
# - Use `USING DELTA` for Delta Lake tables (recommended for Fabric)
# - You can include multiple CREATE TABLE statements in one file (separated by semicolons)
# - Name your files descriptively (e.g., `create_sales_tables.sql`, `create_inventory_tables.sql`)


# CELL ********************

# This cell copies all .sql files from the notebook's resource 'builtin/scripts' folder to the Lakehouse Files/scripts folder.
# It ensures the Lakehouse destination folder exists, then iterates through each .sql file in the source folder.
# Each file's content is read and uploaded to the Lakehouse Files/scripts directory using notebookutils.fs.put.
# After copying all files, it prints a success message for each and a summary completion message.

import os
import shutil

# Paths
lh_scripts_folder = "Files/lakehouse-ddl"
src_folder = scripts_folder  # scripts_folder is defined earlier as the 'builtin/scripts' path

# Ensure Lakehouse Files/scripts folder exists
notebookutils.fs.mkdirs(lh_scripts_folder)

# List all .sql files in the builtin/scripts directory
local_sql_files = [f for f in os.listdir(src_folder) if f.endswith('.sql')]

if not local_sql_files:
    print(f"No .sql files found in {src_folder}")
else:
    for filename in local_sql_files:
        src_file = os.path.join(src_folder, filename)
        dest_path = f"{lh_scripts_folder}/{filename}"
        # Read content from local resource
        with open(src_file, "r") as infile:
            content = infile.read()
        # Write to Lakehouse Files/scripts folder
        notebookutils.fs.put(dest_path, content, overwrite=True)
        print(f"✓ Copied {filename} to {dest_path}")

    print(f"\nAll scripts have been copied to Lakehouse Files/scripts folder.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
