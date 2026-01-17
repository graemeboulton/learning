#!/usr/bin/env python3
"""
Convert pandas notebooks to PySpark notebooks - FINAL version.
Handles code cells more intelligently to avoid duplicate initializations.
"""
import json
import os
import re
from pathlib import Path


def convert_code_cell(code_content, cell_index, notebook_name):
    """Convert pandas code to PySpark code."""
    lines = code_content.split('\n')
    converted_lines = []
    
    # For the first code cell, add PySpark initialization
    is_first_code_cell = (cell_index == 0)
    
    for line in lines:
        # Skip all pandas and numpy imports - we'll handle them globally
        if 'import pandas' in line or 'from pandas' in line:
            continue
        if 'import numpy' in line or 'from numpy' in line:
            continue
        
        # Skip SparkSession imports and initialization on non-first cells
        if not is_first_code_cell:
            if 'SparkSession' in line or ('spark = ' in line and 'appName' in line):
                continue
            if '.getOrCreate()' in line and 'spark' not in line:
                continue
        
        converted_line = line
        
        # pd.read_csv() -> spark.read.csv()
        if 'pd.read_csv(' in converted_line:
            # Replace the pattern
            match = re.search(r'pd\.read_csv\s*\(\s*(["\'])(.+?)\1\s*(?:,\s*([^)]*))?\s*\)', converted_line)
            if match:
                filename = match.group(2)
                params = match.group(3) if match.group(3) else ''
                
                # Handle index_col parameter
                has_index = 'index_col=' in params if params else False
                spark_read = f'spark.read.option("header", "true").option("inferSchema", "true").csv("{filename}")'
                
                if has_index:
                    spark_read += '  # Note: index_col not directly supported; use set operations after reading'
                
                converted_line = re.sub(
                    r'pd\.read_csv\s*\(\s*(["\'])(.+?)\1\s*(?:,\s*([^)]*)?)?\s*\)',
                    spark_read,
                    converted_line
                )
            else:
                converted_line = converted_line.replace('pd.read_csv(', 'spark.read.csv(')
        
        # pd.DataFrame() -> df = spark.createDataFrame()
        if 'pd.DataFrame(' in converted_line and 'spark.createDataFrame' not in converted_line:
            # Match pd.DataFrame(...)
            match = re.search(r'(\w+)\s*=\s*pd\.DataFrame\((.*?)\)', converted_line, re.DOTALL)
            if match:
                var_name = match.group(1)
                data = match.group(2)
                converted_line = f'{var_name} = spark.createDataFrame({data})'
            else:
                converted_line = converted_line.replace('pd.DataFrame(', 'spark.createDataFrame(')
        
        # pd.Series() -> Create DataFrame with single column
        if 'pd.Series(' in converted_line:
            # Try to preserve the logic but comment it
            if 'pd.Series([' in converted_line:
                # Extract list
                match = re.search(r'pd\.Series\(\[(.*?)\]\)', converted_line, re.DOTALL)
                if match:
                    data = match.group(1)
                    converted_line = f'spark.createDataFrame([(x,) for x in [{data}]], ["value"])'
                else:
                    converted_line = converted_line.replace('pd.Series(', 'spark.createDataFrame([')
            else:
                # Comment it out for manual review
                converted_line = '# ' + converted_line + '  # Convert Series manually in PySpark'
        
        # pd.to_datetime() -> Timestamp conversion
        if 'pd.to_datetime(' in converted_line:
            converted_line = converted_line.replace('pd.to_datetime(', 'F.to_timestamp(')
        
        # pd.concat() -> Use union or unionByName
        if 'pd.concat(' in converted_line:
            converted_line = '# PySpark: use df1.union(df2) or spark.sql() for concatenation\n' + converted_line
        
        # .head() -> .show()
        if '.head()' in converted_line:
            converted_line = converted_line.replace('.head()', '.show()')
        elif '.head(' in converted_line:
            match = re.search(r'\.head\s*\(\s*(?:n\s*=\s*)?(\d+)\s*\)', converted_line)
            if match:
                converted_line = re.sub(r'\.head\s*\(\s*(?:n\s*=\s*)?(\d+)\s*\)', r'.show(\1)', converted_line)
        
        # .info() -> .printSchema()
        if '.info()' in converted_line:
            converted_line = converted_line.replace('.info()', '.printSchema()')
        
        # .tail() -> note about limit
        if '.tail()' in converted_line or '.tail(' in converted_line:
            converted_line = '# PySpark: Use .limit(n) or collect() + reverse\n' + converted_line
        
        # .loc -> Comment and note to use filter
        if '.loc[' in converted_line:
            converted_line = '# PySpark: Use .filter() for conditional access\n' + converted_line
        
        # .iloc -> Note about collect and indexing
        if '.iloc[' in converted_line:
            converted_line = '# PySpark: Use .collect()[index] for positional access\n' + converted_line
        
        # .apply() -> Note about UDF or withColumn
        if '.apply(' in converted_line and 'UDF' not in converted_line:
            converted_line = '# PySpark: Use .withColumn() with UDF or F functions\n' + converted_line
        
        # .value_counts() -> groupBy + count
        if '.value_counts()' in converted_line:
            # Try to get context
            if '(' in converted_line and ')' in converted_line:
                converted_line = re.sub(
                    r'(\w+)\.value_counts\(\)',
                    r'df.groupBy("\1").count().orderBy(F.desc("count"))',
                    converted_line
                )
            else:
                converted_line = converted_line.replace('.value_counts()', '.groupBy().count().orderBy(F.desc("count"))')
        
        # .merge() -> .join()
        if '.merge(' in converted_line:
            # Keep the merge call but note about PySpark's join
            converted_line = '# PySpark: Use .join() instead\n' + converted_line.replace('.merge(', '.join(')
        
        # .groupby() -> .groupBy() (capital B)
        if '.groupby(' in converted_line:
            converted_line = converted_line.replace('.groupby(', '.groupBy(')
        
        # .str. -> Note to use F functions
        if '.str.' in converted_line:
            converted_line = '# PySpark: Use pyspark.sql.functions for string operations\n' + converted_line
        
        # .dt. -> Note to use F functions
        if '.dt.' in converted_line:
            converted_line = '# PySpark: Use F.functions for datetime operations\n' + converted_line
        
        # .astype() -> cast()
        if '.astype(' in converted_line:
            match = re.search(r'\.astype\s*\(\s*(["\']?)(\w+)\1\s*\)', converted_line)
            if match:
                dtype = match.group(2)
                converted_line = re.sub(r'\.astype\s*\(\s*["\']?(\w+)["\']?\s*\)', r'.cast(\1)', converted_line)
        
        # .drop() with axis parameter
        if '.drop(' in converted_line and 'axis=' in converted_line:
            converted_line = converted_line.replace('axis="columns"', 'axis=1')
        
        # .sort_values() -> orderBy()
        if '.sort_values(' in converted_line:
            converted_line = '# PySpark: Use .orderBy() instead\n' + converted_line
        
        # .rename() -> withColumnRenamed()
        if '.rename(' in converted_line:
            converted_line = '# PySpark: Use .withColumnRenamed() instead\n' + converted_line
        
        if converted_line.strip():
            converted_lines.append(converted_line)
    
    # Build final output
    result = []
    
    if is_first_code_cell:
        result = [
            'from pyspark.sql import SparkSession',
            'from pyspark.sql import functions as F',
            'from pyspark.sql.types import *',
            'from pyspark.sql import Window',
            '',
            'spark = SparkSession.builder.appName("PySpark_Notebook").getOrCreate()',
            ''
        ]
    
    result.extend(converted_lines)
    
    return '\n'.join(result)


def convert_markdown_cell(content):
    """Convert markdown references from pandas to PySpark."""
    converted = content
    
    # Replace pandas/Pandas with PySpark
    converted = converted.replace('**pandas**', '**PySpark**')
    converted = converted.replace('**Pandas**', '**PySpark**')
    converted = re.sub(r'\bpandas\b', 'PySpark', converted)
    converted = re.sub(r'\bPandas\b', 'PySpark', converted)
    
    # Specific patterns
    converted = converted.replace('A **DataFrame**', 'A PySpark **DataFrame**')
    converted = converted.replace('a **DataFrame**', 'a PySpark **DataFrame**')
    converted = converted.replace('A pandas', 'A PySpark')
    converted = converted.replace('a pandas', 'a PySpark')
    
    # pd. specific references
    converted = re.sub(r'\bpd\.read_csv\(\)', 'spark.read.csv()', converted)
    converted = re.sub(r'\bpd\.Series\b', 'PySpark Column/DataFrame', converted)
    converted = re.sub(r'\bpd\.DataFrame\b', 'PySpark DataFrame', converted)
    
    return converted


def convert_notebook(notebook_path):
    """Convert a single notebook from pandas to PySpark."""
    with open(notebook_path, 'r', encoding='utf-8') as f:
        notebook = json.load(f)
    
    cells = notebook.get('cells', [])
    converted_cells = []
    code_cell_index = 0
    notebook_name = os.path.basename(notebook_path)
    
    for cell in cells:
        if cell['cell_type'] == 'code':
            source = ''.join(cell.get('source', []))
            
            if source.strip():  # Only convert non-empty cells
                converted_source = convert_code_cell(source, code_cell_index, notebook_name)
                code_cell_index += 1
                
                # Properly split and format lines
                lines = converted_source.rstrip('\n').split('\n')
                cell['source'] = [line if i == len(lines) - 1 else line + '\n' 
                                 for i, line in enumerate(lines)]
            else:
                cell['source'] = ['\n'] if cell.get('source') else []
        
        elif cell['cell_type'] == 'markdown':
            source = ''.join(cell.get('source', []))
            converted_source = convert_markdown_cell(source)
            
            lines = converted_source.rstrip('\n').split('\n')
            cell['source'] = [line if i == len(lines) - 1 else line + '\n' 
                             for i, line in enumerate(lines)]
        
        converted_cells.append(cell)
    
    notebook['cells'] = converted_cells
    return notebook


def convert_directory(directory_path, include_playground=True):
    """Convert all notebooks in a directory."""
    notebook_files = sorted(Path(directory_path).glob('*.ipynb'))
    notebook_files = [f for f in notebook_files if '.ipynb_checkpoints' not in str(f)]
    
    if not include_playground:
        notebook_files = [f for f in notebook_files if 'Playground' not in f.name]
    
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
            print(f'    ✗ Error: {str(e)[:60]}')
    
    return converted_count


if __name__ == '__main__':
    complete_pyspark_path = '/Users/graemeboulton/Desktop/Github/Python/pandas/Complete_PySpark'
    incomplete_pyspark_path = '/Users/graemeboulton/Desktop/Github/Python/pandas/Incomplete_PySpark'
    
    print('=' * 80)
    print('FINAL CONVERSION: Pandas Notebooks to PySpark')
    print('=' * 80)
    print()
    
    print('Converting Complete_PySpark notebooks...')
    count1 = convert_directory(complete_pyspark_path, include_playground=True)
    
    print()
    print('Converting Incomplete_PySpark notebooks...')
    count2 = convert_directory(incomplete_pyspark_path, include_playground=False)
    
    print()
    print('=' * 80)
    print(f'✓ Conversion complete!')
    print(f'Total notebooks converted: {count1 + count2}')
    print('=' * 80)
