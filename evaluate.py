import pandas as pd
from sklearn.metrics import r2_score
import os
import yaml

def compute_r2_scores(ground_truth_dir, predicted_dir, output_dir):
    """Calculates R2 scores for common columns in CSV files from two directories and outputs results to text files."""
    # List CSV files in the ground truth directory
    csv_files = [file for file in os.listdir(ground_truth_dir) if file.endswith('.csv')]
    
    for file_name in csv_files:
        # Construct file paths for corresponding files in both directories
        ground_truth_path = os.path.join(ground_truth_dir, file_name)
        predicted_path = os.path.join(predicted_dir, file_name.replace('_prepare.csv', '_process.csv'))

        # Load datasets, dropping columns full of NaN values
        df_ground_truth = pd.read_csv(ground_truth_path).dropna(axis=1, how='all')
        df_predicted = pd.read_csv(predicted_path).dropna(axis=1, how='all')

        # Identify common columns and months between the datasets
        common_columns = set(df_ground_truth.columns).intersection(df_predicted.columns)
        df_ground_truth = df_ground_truth.dropna(subset=common_columns)
        df_predicted = df_predicted.dropna(subset=common_columns)
        common_months = set(df_ground_truth['Month']).intersection(df_predicted['Month'])
        df_ground_truth = df_ground_truth[df_ground_truth['Month'].isin(common_months)]
        df_predicted = df_predicted[df_predicted['Month'].isin(common_months)]

        # Compute R2 scores for common columns
        r2_scores = [r2_score(df_ground_truth[col], df_predicted[col]) for col in common_columns]

        # Determine consistency based on R2 scores
        consistency = 'Consistent' if all(score >= 0.9 for score in r2_scores) else 'Inconsistent'

        # Output results to a text file
        output_file_path = os.path.join(output_dir.rstrip(os.path.sep), file_name.replace('_prepare.csv', '_r2.txt'))
        with open(output_file_path, 'w') as output_file:
            output_file.write(f"{consistency}\n")
            output_file.write(','.join(map(str, r2_scores)))

def main():
    """Main function to orchestrate the R2 score computation based on 'params.yaml' configuration."""
    # Load parameters from the YAML configuration file
    params = yaml.safe_load(open("params.yaml"))
    ground_truth_dir = params["data_prepare"]["dest_folder"]
    predicted_dir = params["data_process"]["dest_folder"]
    output_dir = params["evaluate"]["output"]

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Compute R2 scores
    compute_r2_scores(ground_truth_dir, predicted_dir, output_dir)

if __name__ == "__main__":
    main()
