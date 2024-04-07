import pandas as pd
import yaml
import os

def aggregate_data(input_dir, output_dir, column_mappings_dir):
    """Aggregates daily data to monthly averages, aligning daily and monthly parameters."""
    # List all CSV files in the input directory
    csv_files = [file for file in os.listdir(input_dir) if file.endswith('.csv')]
    
    for file_name in csv_files:
        # Construct the full path to the input file
        input_file_path = os.path.join(input_dir, file_name)

        # Load the dataset
        df = pd.read_csv(input_file_path)

        # Convert DATE column to datetime and extract month
        df['DATE'] = pd.to_datetime(df['DATE'])
        df['Month'] = df['DATE'].dt.month

        # Identify daily and monthly parameter columns
        daily_columns = [col for col in df.columns if 'Daily' in col]
        monthly_columns = []

        # Read the associated monthly parameters from the text file
        column_mapping_path = os.path.join(column_mappings_dir, file_name.replace('.csv', '.txt'))
        with open(column_mapping_path, 'r') as file:
            monthly_columns = file.read().split(',')

        # Map daily parameters to monthly equivalents and retain relevant columns
        columns_to_retain, new_column_names = [], []
        for daily_col in daily_columns:
            param = daily_col.replace('Daily', '')
            for monthly_col in monthly_columns:
                if param in monthly_col or ('Average' in param and monthly_col.replace('Mean', '').replace('Average', '') in param.replace('Average', '')):
                    columns_to_retain.append(daily_col)
                    new_column_names.append(monthly_col)

        # Filter data to include only rows with non-missing values for retained columns
        df_filtered = df.dropna(how='all', subset=columns_to_retain)[['Month'] + columns_to_retain]

        # Aggregate filtered data by month, computing mean for each column
        df_aggregated = df_filtered.groupby('Month').mean().reset_index().rename(columns=dict(zip(columns_to_retain, new_column_names)))

        # Output aggregated data to a CSV file
        output_csv_path = os.path.join(output_dir, file_name.replace('.csv', '_process.csv'))
        df_aggregated.to_csv(output_csv_path, index=False)

def main():
    """Main function to orchestrate the data aggregation process based on 'params.yaml' configuration."""
    # Load parameters from the YAML configuration file
    params = yaml.safe_load(open("params.yaml"))
    input_dir = params["data_source"]["temp_dir"]
    column_mappings_dir = params["data_prepare"]["dest_folder"]
    output_dir = params["data_process"]["dest_folder"]

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Aggregate data
    aggregate_data(input_dir, output_dir, column_mappings_dir)

if __name__ == "__main__":
    main()
