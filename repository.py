
import sys
import os
import importlib.util

# Add the current working directory to the Python path
sys.path.append(os.getcwd())

from dagster import repository

# Dynamic import of my_etl_pipeline module
module_name = 'my_etl_pipeline'
module_path = os.path.join(os.getcwd(), 'my_etl_pipeline.py')

spec = importlib.util.spec_from_file_location(module_name, module_path)
my_etl_pipeline = importlib.util.module_from_spec(spec)
spec.loader.exec_module(my_etl_pipeline)

@repository
def my_repository():
    return [my_etl_pipeline.etl_job]
