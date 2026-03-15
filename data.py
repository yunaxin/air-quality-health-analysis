# Python program to build dataset
import argparse 
import yaml
import logging
import requests
import os
import pandas as pd
from state import state_map

# Helper function for file downloads
def download_file(url, filename):
    try:
        response = requests.get(url)
        response.raise_for_status() 

        with open(filename, 'wb') as f:
            f.write(response.content)
        logging.info(f"File '{filename}' downloaded successfully.")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Download failed: {e}")

# Helper function to load yaml file
def load_yaml(config_path: str):
    try:
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
            return data
    except Exception as e:
        logging.error(f"Unable to load config file with error: {e}")

# Function to handle downloading datasets. Will check if local otherwise download
def retrieve_datasets(config: dict):
    try:
        # Create datasets directory if it doesn't exist
        os.makedirs('./datasets', exist_ok=True)

        epa_data, cdc_data = config['EPA'], config['CDC']

        # Download EPA data
        for item in epa_data:
            year = item['year']
            url = item['url']
            if os.path.exists(f"./datasets/epa_{year}.zip"):
                logging.info(f'"./datasets/epa_{year}.zip exists"')
                continue
            else:
                download_file(url, f"./datasets/epa_{year}.zip")

        # Download CDC data
        for item in cdc_data:
            disease_type = item['name']
            url = item['url']
            output_filename = f"./datasets/cdc_{disease_type}.csv"

            if os.path.exists(output_filename):
                logging.info(f'"{output_filename}" exists')
                continue
            else:
                download_file(url, output_filename)
        
    except Exception as e:
        logging.error(f"Error retrieving datasets: {e}")

def create_dataset(config: dict):
    if os.path.exists("./datasets/processed_dataset.csv"):
        logging.info(f'"./datasets/processed_dataset.csv" already exists')
        return
    # Load EPA dataframe
    epa_df = pd.concat([pd.read_csv(f"./datasets/epa_{item['year']}.zip",compression='zip') for item in config['EPA']],ignore_index=True)
    # clean EPA
    epa_df["Year"] = epa_df["Year"].astype(int)
    # Load CDC dataframe
    cdc_df = pd.concat([pd.read_csv(f"./datasets/cdc_{item['name']}.csv").assign(Cause=item['name']) for item in config['CDC']],ignore_index=True)
    # get country/state from cdc and other cleaning
    cdc_df = cdc_df.dropna(subset=["County", "Year"])
    # handle missing population data
    cdc_df["Population"] = pd.to_numeric(cdc_df["Population"], errors="coerce").astype("Int64")
    cdc_df["Crude Rate"] = pd.to_numeric(cdc_df["Crude Rate"], errors="coerce")
    cdc_df[["county_raw", "state_abbr"]] = (
        cdc_df["County"]
        .str.split(",", expand=True)
    )
    cdc_df["County"] = (
        cdc_df["county_raw"]
        .str.replace(" County", "", regex=False)
        .str.strip()
    )
    cdc_df["state_abbr"] = cdc_df["state_abbr"].str.strip()
    cdc_df["State"] = cdc_df["state_abbr"].map(state_map)
    
    # Join datasets
    processed_df = epa_df.merge(
        cdc_df,
        on=["State", "County", "Year"],
        how="inner"
    )

    cols_to_drop = [
        "county_raw",
        "state_abbr",
        "Notes",
        "Year Code",
        "County Code", 
        'Crude Rate Lower 95% Confidence Interval',
        'Crude Rate Upper 95% Confidence Interval',
        'Crude Rate Lower 95% Confidence Interval'
    ]

    processed_df = processed_df.drop(
        columns=[c for c in cols_to_drop if c in processed_df.columns]
    )

    processed_df.to_csv("./datasets/processed_dataset.csv", index=False)
    logging.info('Created "./datasets/processed_dataset.csv"')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dataset preparation program")
    parser.add_argument('-config',type=str, default='./datasets/data_sources.yaml')

    args = parser.parse_args()

    config_path = args.config

    logging.basicConfig(level=logging.DEBUG)
    logging.info(f" Using dataset config file located at: {config_path}")

    config = load_yaml(config_path)
    retrieve_datasets(config)
    create_dataset(config)