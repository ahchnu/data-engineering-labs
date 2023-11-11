import json
import csv
import os
from glob import glob

def flatten_json(json_obj, prefix=''):
    flat_dict = {}
    for key, value in json_obj.items():
        if isinstance(value, dict):
            flat_dict.update(flatten_json(value, f"{prefix}{key}_"))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                flat_dict[f"{prefix}{key}_{i}"] = item
        else:
            flat_dict[f"{prefix}{key}"] = value
    return flat_dict

def process_json_file(json_file):
    with open(json_file, 'r') as file:
        data = json.load(file)

    flattened_data = flatten_json(data)

    csv_filename = os.path.splitext(json_file)[0] + '.csv'

    with open(csv_filename, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=flattened_data.keys())
        csv_writer.writeheader()
        csv_writer.writerow(flattened_data)

    print(f"Converted {json_file} to {csv_filename}")

def main():
    data_folder = 'data'
    json_files = glob(os.path.join(data_folder, '**/*.json'), recursive=True)

    for json_file in json_files:
        process_json_file(json_file)

if __name__ == "__main__":
    main()
