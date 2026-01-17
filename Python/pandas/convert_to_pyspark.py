#!/usr/bin/env python3
"""
Convert pandas notebooks to PySpark notebooks.
"""
import json
import os
import re
from pathlib import Path


def convert_code_cell(code_content):
    """Convert pandas code to PySpark code."""
    lines = code_content.split('\n')
    converted_lines = []
    
    # Track if we've already added SparkSession initialization
    has_spark_init = any('SparkSession' in line for line in lines)
    
    for i, line in enumerate(lines):
        # Replace pandas imports with PySpark imports
        if 'import pandas as pd' in line:
            # Skip the pandas import, we'll handle it specially
            continue
        elif 'import numpy as np' in line:
            # Keep numpy if it exists
            converted_lines.append(line)
        elif 'from pandas' in line:
            # Skip pandas-specific imports
            continue
        else:
            # Convert pandas operations
            converted_line = line
            
            # pd.read_csv() -> spark.read.csv()
            converted_line = re.sub(
                r'pd\.read_csv\s*\(\s*(["\'])(.+?)\1\s*(?:,\s*([^)]*))?\)',
                lambda m: f'spark.read.csv("{m.group(2)}", header=True, inferSchema=True)',
                converted_line
            )
            
            # pd.DataFrame -> spark.createDataFrame
            converted_line = converted_line.replace('pd.DataFrame', 'spark.createDataFrame')
            
            # pd.Series -> pyspark Column operations
            if 'pd.Series(' in converted_line:
                converted_line = converted_line.replace('pd.Series(', '# Note: PySpark uses columns instead of Series\n# ')
            
            # pd.to_datetime -> for PySpark, use F.to_timestamp
            converted_line = converted_line.replace('pd.to_datetime', 'F.to_timestamp')
            
            # .head() -> .show()
            converted_line = converted_line.replace('.head()', '.show()')
            converted_line = re.sub(r'\.head\((\d+)\)', r'.show(\1)', converted_line)
            
            # .tail() -> .tail() exists in PySpark but different
            # For now, keep tail as is for comment
            
            # .info() -> .printSchema()
            converted_line = converted_line.replace('.info()', '.printSchema()')
            
            # .describe() -> .describe()
            # .describe() exists in PySpark, keep it
            
            # .loc, .iloc -> need comments about filter/select
            if '.loc[' in converted_line or '.iloc[' in converted_line:
                converted_line = '# PySpark uses .filter() and .select() instead of .loc/.iloc\n' + converted_line
            
            # .apply() -> .withColumn() with F.udf or F.when
            if '.apply(' in converted_line:
                converted_line = '# Use .withColumn() with PySpark functions instead of .apply()\n' + converted_line
            
            # .value_counts() -> .groupBy().count()
            if '.value_counts()' in converted_line:
                converted_line = converted_line.replace('.value_counts()', '.groupBy().count().sort("count", ascending=False)')
            
            # .merge() -> .join()
            if '.merge(' in converted_line:
                converted_line = '# Use .join() for merging DataFrames in PySpark\n' + converted_line
            
            # .concat -> Use union or unionByName
            if 'pd.concat(' in converted_line:
                converted_line = '# Use df.union() or df.unionByName() in PySpark\n' + converted_line
            
            # .str. -> F.col().like() or other string functions
            if '.str.' in converted_line:
                converted_line = '# Use pyspark.sql.functions string functions for string operations\n' + converted_line
            
            # .groupby() -> .groupBy() (note the capital B)
            converted_line = converted_line.replace('.groupby(', '.groupBy(')
            
            converted_lines.append(converted_line)
    
    # Add PySpark imports at the beginning if not already present
    result_lines = []
    if not has_spark_init and lines and lines[0].strip():
        result_lines = [
            'from pyspark.sql import SparkSession',
            'from pyspark.sql import functions as F',
            'from pyspark.sql.types import *',
            'from pyspark.sql import Window',
            '',
            'spark = SparkSession.builder.appName("pandas_to_pyspark").getOrCreate()',
            ''
        ]
    
    result_lines.extend(converted_lines)
    return '\n'.join(result_lines)


def convert_markdown_cell(content):
    """Convert markdown references from pandas to PySpark."""
    converted = content
    
    # Replace pandas references with PySpark
    converted = converted.replace('**pandas**', '**PySpark**')
    converted = converted.replace('**Pandas**', '**PySpark**')
    converted = converted.replace('pandas ', 'PySpark ')
    converted = converted.replace('Pandas ', 'PySpark ')
    converted = converted.replace('pd.', 'spark.')
    
    # Update some specific descriptions
    converted = converted.replace('A pandas **Series**', 'A PySpark **Column** or single-column DataFrame')
    converted = converted.replace('a pandas **Series**', 'a PySpark **Column** or single-column DataFrame')
    converted = converted.replace('**DataFrame**', '**DataFrame**')  # Keep DataFrame as is
    
    return converted


def convert_notebook(notebook_path):
    """Convert a single notebook from pandas to PySpark."""
    with open(notebook_path, 'r') as f:
        notebook = json.load(f)
    
    cells = notebook.get('cells', [])
    converted_cells = []
    import_handled = False
    
    for cell in cells:
        if cell['cell_type'] == 'code':
            source = ''.join(cell.get('source', []))
            
            # Add PySpark imports at the very first code cell
            if not import_handled and source.strip():
                converted_source = 'from pyspark.sql import SparkSession\n'
                converted_source += 'from pyspark.sql import functions as F\n'
                converted_source += 'from pyspark.sql.types import *\n'
                converted_source += 'from pyspark.sql import Window\n'
                converted_source += '\n'
                converted_source += 'spark = SparkSession.builder.appName("PySpark_Conversion").getOrCreate()\n'
                converted_source += '\n'
                converted_source += convert_code_cell(source)
                import_handled = True
            else:
                converted_source = convert_code_cell(source)
            
            cell['source'] = converted_source.split('\n')
            # Ensure each line ends with newline
            cell['source'] = [line + '\n' if i < len(cell['source']) - 1 else line 
                             for i, line in enumerate(cell['source'])]
        
        elif cell['cell_type'] == 'markdown':
            source = ''.join(cell.get('source', []))
            converted_source = convert_markdown_cell(source)
            cell['source'] = converted_source.split('\n')
            # Ensure each line ends with newline
            cell['source'] = [line + '\n' if i < len(cell['source']) - 1 else line 
                             for i, line in enumerate(cell['source'])]
        
        converted_cells.append(cell)
    
    notebook['cells'] = converted_cells
    
    return notebook


def convert_directory(directory_path):
    """Convert all notebooks in a directory."""
    notebook_files = sorted(Path(directory_path).glob('*.ipynb'))
    notebook_files = [f for f in notebook_files if '.ipynb_checkpoints' not in str(f)]
    
    for notebook_file in notebook_files:
        print(f'Converting: {notebook_file.name}...')
        try:
            converted = convert_notebook(str(notebook_file))
            with open(str(notebook_file), 'w') as f:
                json.dump(converted, f, indent=1)
            print(f'  ✓ Converted successfully')
        except Exception as e:
            print(f'  ✗ Error: {e}')
    
    return len(notebook_files)


if __name__ == '__main__':
    complete_pyspark_path = '/Users/graemeboulton/Desktop/Github/Python/pandas/Complete_PySpark'
    incomplete_pyspark_path = '/Users/graemeboulton/Desktop/Github/Python/pandas/Incomplete_PySpark'
    
    print('=' * 80)
    print('Converting Complete_PySpark notebooks...')
    print('=' * 80)
    count1 = convert_directory(complete_pyspark_path)
    
    print('\n' + '=' * 80)
    print('Converting Incomplete_PySpark notebooks...')
    print('=' * 80)
    count2 = convert_directory(incomplete_pyspark_path)
    
    print('\n' + '=' * 80)
    print(f'Conversion complete!')
    print(f'Total notebooks converted: {count1 + count2}')
    print('=' * 80)
