import pandas as pd  # type: ignore
import os
import logging

from sklearn.ensemble import IsolationForest # type: ignore
from sklearn.impute import KNNImputer # type: ignore

DATA_PATH = "/opt/airflow/data/tourism_data.csv"
OUTPUT_PATH = "/opt/airflow/data/cleaned_tourism_data.csv"

#DATA_PATH = r"C:\Users\cyril\airflow_tourism_pipeline\data\tourism_data.csv"
#OUTPUT_PATH = r"C:\Users\cyril\airflow_tourism_pipeline\data\cleaned_tourism_data.csv"

def clean_data():
    """cleans the tourism data by removing rows with missing values and duplicates, then saves the cleaned data to a new CSV file."""
    try:
        # Load the data
        df = pd.read_csv(DATA_PATH)

        # Remove rows with missing values
        df.dropna(inplace=True)
        logging.info("Removed rows with missing values.")
    
        # Remove duplicate rows
        df.drop_duplicates(inplace=True)
        logging.info("Removed duplicate rows.")

        return df
    except Exception as e:
        logging.error(f"An error occurred while cleaning the data: {e}")    
        return None
    
def fix_data_types(df):
    """Fixes the data types of the columns in the tourism data."""
    
    try:
        df = df.copy()

        # Convert to best possible dtypes
        df = df.convert_dtypes()

        # Try converting numeric columns
        df = df.apply(lambda x: pd.to_numeric(x, errors='ignore'))

        # Detect date/time columns
        date_cols = [c for c in df.columns if c.lower().endswith(('date', 'time'))]

        if date_cols:
            for col in date_cols:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                logging.info(f"Converted '{col}' column to datetime.")
        else:
            logging.warning("No date columns found to convert.")

        # Convert object columns to pandas string dtype
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype('string')
            logging.info(f"Converted '{col}' column to string.")

        return df

    except Exception as e:
        logging.error(f"An error occurred while fixing data types: {e}")
        return df
    
def standardize_column_names(df):
    """Standardizes column names by converting them to lowercase and replacing spaces with underscores."""
    try:
        df = df.copy()
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
        logging.info("Standardized column names.")
        return df
    except Exception as e:
        logging.error(f"An error occurred while standardizing column names: {e}")
        return df
    
def validate_data(df):
    """Validates the cleaned data."""
    try:
        # validation: Check if there are any rows left after cleaning
        if df.empty:
            logging.warning("No data available for validation.")
            return False

        # validation: Check for any remaining missing values
        if df.isnull().sum().sum() > 0:
            logging.error("Data contains missing values.")
            return False
        
        if df.duplicated().sum() > 0:
            logging.error("Data contains duplicate rows.")
            return False

        logging.info("Data validation passed.")
        return True
    

    except Exception as e:
        logging.error(f"An error occurred while validating the data: {e}")
        return False
    
def detect_anomalies(df, numeric_cols, contamination=0.02):
    """Detects anomalies in the data using Isolation Forest."""
    try:
        df = df.copy()
        if not numeric_cols:
            logging.warning("No numeric columns provided for anomaly detection.")
            return df

        # Ensure numeric columns are of numeric dtype
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Drop rows with NaN values in numeric columns
        df.dropna(subset=numeric_cols, inplace=True)

        if df.empty:
            logging.warning("No data available after dropping NaN values for anomaly detection.")
            return df

        # Fit Isolation Forest
        iso_forest = IsolationForest(contamination=contamination, random_state=42)
        df['anomaly'] = iso_forest.fit_predict(df[numeric_cols])

        # Filter out anomalies
        cleaned_df = df[df['anomaly'] == 1].drop(columns=['anomaly'])
        logging.info(f"Detected and removed {len(df) - len(cleaned_df)} anomalies.")
        
        return cleaned_df

    except Exception as e:
        logging.error(f"An error occurred while detecting anomalies: {e}")
        return df
    
def run_cleaning_pipeline():
    """Runs the entire data cleaning pipeline."""
    df = clean_data()
    if df is not None:
        df = fix_data_types(df)
        df = standardize_column_names(df)
        if validate_data(df):
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            df = detect_anomalies(df, numeric_cols)
            df.to_csv(OUTPUT_PATH, index=False)
            logging.info(f"Cleaned data saved to {OUTPUT_PATH}.")
        else:
            logging.error("Data validation failed. Cleaned data will not be saved.")
    else:
        logging.error("Data cleaning failed. No cleaned data to save.")
