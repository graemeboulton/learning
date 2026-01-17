#!/usr/bin/env python3
"""
Convert pandas notebooks to PySpark notebooks - Enhanced version.
"""
import json
import os
import re
from pathlib import Path


def convert_code_cell(code_content, cell_index):
    """Convert pandas code to PySpark code."""
    lines = code_content.split('\n')
    converted_lines = []
    
    # For the first code cell, add PySpark initialization
    if cell_index == 0:is 
        converted_lines = [
            'from pyspark.sql import SparkSession',
            'from pyspark.sql import functions as F',
            'from pyspark.sql.types import *',
            'from pyspark.sql import Window',
            '',
            'spark = SparkSession.builder.appName("PySpark_Conversion").getOrCreate()',
            ''
        ]
    
    for line in lines:
        # Skip pandas imports - they're already replaced
        if 'import pandas' in line or 'from pandas' in line:
            continue
        
        # Skip numpy import if alone
        if 'import numpy' in line:
            # We could keep this but PySpark often has alternatives
            continue
        
        # Skip duplicate spark session initialization
        if 'SparkSession' in line or 'spark = ' in line or '.getOrCreate()' in line:
            if not converted_lines or 'PySpark_Conversion' not in '\n'.join(converted_lines):
                converted_lines.append(line)
            continue
        
        converted_line = line
        
        # pd.read_csv() -> spark.read.csv()
        if 'pd.read_csv(' in converted_line:
            # Extract filename and parameters
            match = re.search(r'pd\.read_csv\s*\(\s*(["\'])(.+?)\1\s*(?:,\s*([^)]*))?\s*\)', converted_line)
            if match:
                filename = match.group(2)
                params = match.group(3) if match.group(3) else ''
                converted_line = f'spark.read.option("inferSchema", "true").csv("{filename}", header=True)'
                if params:
                    # Try to handle index_col parameter
                    if 'index_col=' in params:
                        converted_line += '  # Note: Use .filter() or set as index differently in PySpark'
            else:
                # Fallback
                converted_line = converted_line.replace('pd.read_csv(', 'spark.read.csv(')
        
        # pd.DataFrame -> spark.createDataFrame
        if 'pd.DataFrame(' in converted_line:
            converted_line = converted_line.replace('pd.DataFrame(', 'spark.createDataFrame(')
        
        # pd.Series -> Comment and show PySpark alternative
        if 'pd.Series(' in converted_line:
            # Extract the data
            match = re.search(r'pd\.Series\((.*?)\)', converted_line, re.DOTALL)
            if match:
                data = match.group(1)
                converted_line = f'spark.createDataFrame([(d,) for d in {data}], ["value"])'
            else:
                converted_line = converted_line.replace('pd.Series(', 'spark.createDataFrame([')
        
        # pd.to_datetime -> F.to_timestamp or F.to_date
        if 'pd.to_datetime(' in converted_line:
            converted_line = converted_line.replace('pd.to_datetime(', 'F.to_timestamp(')
        
        # pd.concat -> Use union or unionByName
        if 'pd.concat(' in converted_line:
            converted_line = converted_line.replace('pd.concat(', '# PySpark: use df1.union(df2) or df1.unionByName(df2)\n# ').replace('([', '[').replace('])', ']')
        
        # .head() -> .show()
        if '.head()' in converted_line:
            converted_line = converted_line.replace('.head()', '.show()')
        elif '.head(' in converted_line:
            converted_line = re.sub(r'\.head\s*\(\s*(\d+)\s*\)', r'.show(\1)', converted_line)
        
        # .info() -> .printSchema()
        if '.info()' in converted_line:
            converted_line = converted_line.replace('.info()', '.printSchema()')
        
        # .tail() -> show last N (PySpark doesn't have direct .tail())
        if '.tail()' in converted_line:
            converted_line = converted_line.replace('.tail()', '# Note: PySpark doesn\'t have .tail(); use .limit() with reverse order')
        
        # .loc[...] -> .filter() or select
        if '.loc[' in converted_line:
            converted_line = '# Use .filter() for conditional access instead of .loc\n' + converted_line
        
        # .iloc[...] -> .collect()[index]
        if '.iloc[' in converted_line:
            converted_line = '# Use .collect()[index] for positional access instead of .iloc\n' + converted_line
        
        # .apply() -> Use F.udf or .withColumn()
        if '.apply(' in converted_line:
            converted_line = '# Use .withColumn() with UDF or PySpark functions instead of .apply()\n' + converted_line
        
        # .value_counts() -> .groupBy().count()
        if '.value_counts()' in converted_line:
            # Get the column name if possible
            col_match = re.search(r'(\w+)\.value_counts\(\)', converted_line)
            if col_match:
                col_name = col_match.group(1)
                converted_line = re.sub(
                    r'(\w+)\.value_counts\(\)',
                    r'df.groupBy("\1").count().orderBy(F.desc("count"))',
                    converted_line
                )
            else:
                converted_line = converted_line.replace('.value_counts()', '.groupBy().count().orderBy(F.desc("count"))')
        
        # .merge() -> .join()
        if '.merge(' in converted_line:
            converted_line = converted_line.replace('.merge(', '.join(')
        
        # .groupby() -> .groupBy()
        if '.groupby(' in converted_line:
            converted_line = converted_line.replace('.groupby(', '.groupBy(')
        
        # .str. operations -> F.col().like() or other string functions
        if '.str.' in converted_line:
            converted_line = '# Use pyspark.sql.functions for string operations\n' + converted_line
        
        # .dt. operations -> F functions
        if '.dt.' in converted_line:
            converted_line = '# Use pyspark.sql.functions (F) for date/time operations\n' + converted_line
        
        # .astype() -> .cast()
        if '.astype(' in converted_line:
            converted_line = converted_line.replace('.astype(', '.cast(')
        
        # .drop() -> same in PySpark but syntax may differ
        if '.drop(' in converted_line and 'axis=' in converted_line:
            converted_line = converted_line.replace('axis="columns"', 'axis=1')
        
        # .sort_values() -> .orderBy()
        if '.sort_values(' in converted_line:
            converted_line = '# Use .orderBy() instead of .sort_values()\n' + converted_line
        
        # .rename() -> .withColumnRenamed()
        if '.rename(' in converted_line:
            converted_line = '# Use .withColumnRenamed() instead of .rename()\n' + converted_line
        
        # .fillna() -> .fillna() exists in PySpark too
        # .dropna() -> .dropna() exists in PySpark
        
        # .index -> use row_number() window function or collect
        if '.index' in converted_line and 'for' not in converted_line:
            if '.index' in converted_line and not 'index=' in converted_line:
                pass  # Keep for now as it might be reference
        
        # .columns -> .columns (same in PySpark)
        # .dtypes -> .dtypes (similar in PySpark)
        # .shape -> use .count() and len(.columns)
        
        if converted_line.strip():  # Only add non-empty lines
            converted_lines.append(converted_line)
    
    return '\n'.join(converted_lines)


def convert_markdown_cell(content):
    """Convert markdown references from pandas to PySpark."""
    converted = content
    
    # Replace pandas references with PySpark
    if 'pandas' in converted.lower() or 'pd.' in converted:
        converted = converted.replace('**pandas**', '**PySpark/Apache Spark**')
        converted = converted.replace('**Pandas**', '**PySpark**')
        converted = converted.replace('*pandas*', '*PySpark*')
        converted = converted.replace(' pandas ', ' PySpark ')
        
        # Replace pd. references with spark. or F. depending on context
        converted = re.sub(r'\bpd\.read_csv\(\)', 'spark.read.csv()', converted)
        converted = re.sub(r'\bpd\.Series\b', 'PySpark Column/DataFrame', converted)
        converted = re.sub(r'\bpd\.DataFrame\b', 'PySpark DataFrame', converted)
        
        # Update some specific descriptions
        converted = converted.replace('A **DataFrame**', 'A PySpark **DataFrame**')
        converted = converted.replace('a **DataFrame**', 'a PySpark **DataFrame**')
    
    return converted


def convert_notebook(notebook_path):
    """Convert a single notebook from pandas to PySpark."""
    with open(notebook_path, 'r', encoding='utf-8') as f:
        notebook = json.load(f)
    
    cells = notebook.get('cells', [])
    converted_cells = []
    code_cell_count = 0
    
    for cell in cells:
        if cell['cell_type'] == 'code':
            source = ''.join(cell.get('source', []))
            converted_source = convert_code_cell(source, code_cell_count)
            code_cell_count += 1
            
            # Split and rejoin with proper line ending handling
            cell['source'] = converted_source.rstrip('\n').split('\n')
            cell['source'] = [line if i == len(cell['source']) - 1 else line + '\n' 
                             for i, line in enumerate(cell['source'])]
        
        elif cell['cell_type'] == 'markdown':
            source = ''.join(cell.get('source', []))
            converted_source = convert_markdown_cell(source)
            cell['source'] = converted_source.rstrip('\n').split('\n')
            cell['source'] = [line if i == len(cell['source']) - 1 else line + '\n' 
                             for i, line in enumerate(cell['source'])]
        
        converted_cells.append(cell)
    
    notebook['cells'] = converted_cells
    
    return notebook


def convert_directory(directory_path):
    """Convert all notebooks in a directory."""
    notebook_files = sorted(Path(directory_path).glob('*.ipynb'))
    notebook_files = [f for f in notebook_files if '.ipynb_checkpoints' not in str(f) and 'Playground' not in f.name]
    
    converted_count = 0
    for notebook_file in notebook_files:
        print(f'  Converting: {notebook_file.name}...')
        try:
            converted = convert_notebook(str(notebook_file))
            with open(str(notebook_file), 'w', encoding='utf-8') as f:
                json.dump(converted, f, indent=1, ensure_ascii=False)
            print(f'    ✓ Success')
            converted_count += 1
        except Exception as e:
            print(f'    ✗ Error: {e}')
    
    return converted_count


if __name__ == '__main__':
    complete_pyspark_path = '/Users/graemeboulton/Desktop/Github/Python/pandas/Complete_PySpark'
    incomplete_pyspark_path = '/Users/graemeboulton/Desktop/Github/Python/pandas/Incomplete_PySpark'
    
    print('=' * 80)
    print('ENHANCED CONVERSION: Pandas Notebooks to PySpark')
    print('=' * 80)
    print()
    
    print('Converting Complete_PySpark notebooks...')
    count1 = convert_directory(complete_pyspark_path)
    
    print()
    print('Converting Incomplete_PySpark notebooks...')
    count2 = convert_directory(incomplete_pyspark_path)
    
    print()
    print('=' * 80)
    print(f'✓ Conversion complete!')
    print(f'Total notebooks converted: {count1 + count2}')
    print('=' * 80)
