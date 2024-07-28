
import pandas as pd
from sklearn.preprocessing import PolynomialFeatures
from dagster import job, op
from sqlalchemy import create_engine
import numpy as np

# Paths to the datasets
file_path_1 = "Galway bay test site.csv"
file_path_2 = "Wave buoy_Sligo.csv"

# Define the connection string
connection_string = "postgresql://dap:dap@127.0.0.1:5432/Wave energy output"
batch_size = 1000

@op
def galway_bay_test_site_extract_data(context):
    test_site_data = pd.read_csv(file_path_1, low_memory=False)
    context.log.info(f"Extracted {len(test_site_data)} rows from {file_path_1}")
    return test_site_data

@op
def wave_buoy_sligo_extract_data(context):
    buoy_data = pd.read_csv(file_path_2, low_memory=False)
    context.log.info(f"Extracted {len(buoy_data)} rows from {file_path_2}")
    return buoy_data

@op
def galway_bay_test_site_clean_data(context, data):
    data_cleaned = data.apply(pd.to_numeric, errors='coerce')  # Convert non-numeric to NaN
    data_cleaned = data_cleaned.fillna(data_cleaned.mean(numeric_only=True))  # Fill NaN values with column means
    context.log.info("Data cleaned successfully for dataset 1")
    return data_cleaned

@op
def wave_buoy_sligo_clean_data(context, data):
    columns_to_drop = [
        'longitude', 'latitude', 'station_id'
    ]
    data_cleaned = data.drop(columns=columns_to_drop, errors='ignore')
    data_cleaned = data_cleaned.apply(pd.to_numeric, errors='coerce')  # Convert non-numeric to NaN
    data_cleaned = data_cleaned.fillna(data_cleaned.mean(numeric_only=True))  # Fill NaN values with column means
    context.log.info("Data cleaned successfully for dataset 2")
    return data_cleaned

@op
def galway_bay_test_site_transform_data(context, data):
    target_column = 'Power_Output'
    required_columns = [
        "X1", "X2", "X3", "X4", "X5", "X6", "X7", "X8", "X9", "X10", "X11", "X12", "X13", "X14", "X15", "X16",
        "Y1", "Y2", "Y3", "Y4", "Y5", "Y6", "Y7", "Y8", "Y9", "Y10", "Y11", "Y12", "Y13", "Y14", "Y15", "Y16",
        "Power_Output", "wave_height"
    ]
    
    # Sample the data
    data_sample = data.sample(frac=0.1, random_state=42)
    
    # Creating the wave_height column
    p_columns = [f'P{i}' for i in range(1, 17)]
    data_sample['wave_height'] = data_sample[p_columns].sum(axis=1) + 1
    
    # Combine original data with wave_height
    data_transformed = data_sample[required_columns]

    context.log.info("Data transformed successfully for dataset 1")
    return data_transformed

@op
def wave_buoy_sligo_transform_data(context, data):
    target_column = 'SignificantWaveHeight'
    
    # Sample the data
    data_sample = data.sample(frac=0.1, random_state=42)
    
    # Convert 'time' column to datetime
    if 'time' in data_sample.columns:
        data_sample['time'] = pd.to_datetime(data_sample['time'], errors='coerce')
    else:
        context.log.warning("'time' column not found in the dataset.")
    
    # Drop columns with high percentage of NaN values
    nan_threshold = 0.5
    data_sample = data_sample.dropna(thresh=int(nan_threshold * len(data_sample)), axis=1)
    
    context.log.info(f"Columns after dropping those with more than {nan_threshold * 100}% NaNs: {data_sample.columns.tolist()}")

    # Ensure there are still rows left after dropping NaNs
    if data_sample.empty:
        context.log.error("Data sample is empty after dropping columns with high NaN values.")
        return pd.DataFrame()  # Return an empty DataFrame to handle gracefully

    # Creating the WavePeriod column
    data_sample['WavePeriod'] = data_sample['PeakPeriod'] + data_sample['UpcrossPeriod']
    data_sample = data_sample.drop(columns=['PeakPeriod', 'UpcrossPeriod'])

    # Combine original data with WavePeriod
    data_transformed = data_sample

    # Re-add the 'time' column if it exists
    if 'time' in data_sample.columns:
        data_transformed['time'] = data_sample['time'].iloc[data_sample.shape[0] - data_transformed.shape[0]:].values
    else:
        # Create a new 'time' column with random timestamps from 2006 to 2024
        timestamps = pd.date_range(start='2006-01-01', end='2024-12-31', periods=len(data_transformed))
        np.random.shuffle(timestamps.values)
        data_transformed['time'] = timestamps

    context.log.info("Data transformed successfully for dataset 2")
    return data_transformed

@op
def join_datasets(context, test_site_data, buoy_data):
    # Join the datasets by row-wise concatenation
    data_combined = pd.concat([test_site_data.reset_index(drop=True), buoy_data.reset_index(drop=True)], axis=1)
    context.log.info("Datasets joined successfully")
    return data_combined

@op
def load_data(context, data_combined):
    # Create a connection to the database
    engine = create_engine(connection_string)
    
    # Insert data in smaller batches
    for i in range(0, len(data_combined), batch_size):
        batch = data_combined.iloc[i:i+batch_size]
        batch.to_sql('test', engine, index=False, if_exists='append' if i > 0 else 'replace')
        context.log.info(f"Batch {i//batch_size + 1} loaded into PostgreSQL successfully")
        
@job
def etl_job():
    test_site_data = galway_bay_test_site_extract_data()
    cleaned_test_site_data = galway_bay_test_site_clean_data(test_site_data)
    transformed_test_site_data = galway_bay_test_site_transform_data(cleaned_test_site_data)
    
    buoy_data = wave_buoy_sligo_extract_data()
    cleaned_buoy_data = wave_buoy_sligo_clean_data(buoy_data)
    transformed_buoy_data = wave_buoy_sligo_transform_data(cleaned_buoy_data)
    
    data_combined = join_datasets(transformed_test_site_data, transformed_buoy_data)
    load_data(data_combined)
