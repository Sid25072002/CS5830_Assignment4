import pandas as pd
import yaml
import os

def process_monthly_data(input_dir, output_dir):
    """Processes CSV files to extract and transform monthly parameters, then outputs to CSV and text files."""
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

        # Identify monthly parameter columns
        monthly_columns = [col for col in df.columns if 'Monthly' in col]
        monthly_parameters = []
        for col in monthly_columns:
            # Extract parameter name and adjust naming conventions
            param = col.replace('Monthly', '')
            if 'WetBulb' not in param and 'Departure' not in param:
                param = param.replace('Temperature', 'DryBulbTemperature')
            monthly_parameters.append(param)

        # Filter data to include only rows with non-missing monthly values
        df_monthly = df.dropna(how='all', subset=monthly_columns)[['Month'] + monthly_columns]

        # Output processed data to a CSV file
        output_csv_path = os.path.join(output_dir, file_name.replace('.csv', '_prepare.csv'))
        df_monthly.to_csv(output_csv_path, index=False)

        # Output monthly parameters to a text file
        output_txt_path = os.path.join(output_dir, file_name.replace('.csv', '.txt'))
        with open(output_txt_path, 'w') as txt_file:
            txt_file.write(','.join(monthly_parameters))

def main():
    """Main function to orchestrate the processing of monthly data based on 'params.yaml' configuration."""
    # Load parameters from the YAML configuration file
    params = yaml.safe_load(open("params.yaml"))
    input_dir = params["data_source"]["temp_dir"]
    output_dir = params["data_prepare"]["dest_folder"]

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Process monthly data
    process_monthly_data(input_dir, output_dir)

if __name__ == "__main__":
    main()
