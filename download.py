# Import necessary libraries
import subprocess
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm import tqdm
import yaml

def fetch_and_parse_html(base_url, year, output_file):
    """Downloads an HTML page and parses it for CSV file links."""
    # Use curl to download the HTML page containing links to CSV files
    subprocess.run(["curl", "-L", "-o", output_file, f"{base_url}{year}"])

    # Read the downloaded HTML file
    with open(output_file, 'r') as file:
        html_content = file.read()

    # Parse HTML to extract links
    soup = BeautifulSoup(html_content, 'html.parser')
    return soup

def extract_csv_links(parse_file, base_url, year, max_files):
    """Extracts CSV file links from parsed HTML content based on memory size."""
    csv_links = []
    # Iterate through table rows, skipping header rows
    for row in parse_file.find_all('tr')[2:]:
        cells = row.find_all('td')
        # Ensure row has columns and the third column ends with 'M' (indicating memory size in MB)
        if cells and cells[2].text.strip().endswith('M'):
            link = urljoin(f"{base_url}{year}/", cells[0].text.strip())
            memory_size = float(cells[2].text.strip().replace('M', ''))
            csv_links.append((link, memory_size))

    # Filter and return links for CSV files larger than 45MB, up to the max_files limit
    return [link for link, memory in csv_links if memory > 45][:max_files]

def download_csv_files(csv_links, temp_dir):
    """Downloads CSV files from provided links and saves them to a temporary directory."""
    os.makedirs(temp_dir, exist_ok=True)  # Ensure temporary directory exists

    # Download each CSV file
    for link in csv_links:
        response = requests.get(link)
        if response.status_code == 200:  # Check for successful response
            file_path = os.path.join(temp_dir, os.path.basename(link))
            with open(file_path, 'wb') as file, tqdm(
                desc=os.path.basename(link),
                total=int(response.headers.get('content-length', 0)),
                unit='iB',
                unit_scale=True
            ) as bar:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
                    bar.update(len(chunk))

def main():
    """Main function to orchestrate the download process based on configuration in 'params.yaml'."""
    # Load parameters from YAML configuration file
    params = yaml.safe_load(open("params.yaml"))
    base_url = params["data_source"]["base_url"]
    year = params["data_source"]["year"]
    output_file = params["data_source"]["output"]
    temp_dir = params["data_source"]["temp_dir"]
    max_files = params["data_source"]["max_files"]

    # Process HTML to extract CSV links and download files
    parsed_html = fetch_and_parse_html(base_url, year, output_file)
    csv_links = extract_csv_links(parsed_html, base_url, year, max_files)
    download_csv_files(csv_links, temp_dir)

if __name__ == "__main__":
    main()
