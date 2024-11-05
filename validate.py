import pandas as pd
import json
from pathlib import Path

CURR_DIR = Path(__file__).parent

def read_data(input_file):
    input_ext = Path(input_file).suffix.lower()
    if input_ext == '.csv':
        return pd.read_csv(input_file)
    elif input_ext == '.json':
        return pd.read_json(input_file)
    elif input_ext == '.xlsx':
        return pd.read_excel(input_file)
    elif input_ext == '.tsv':
        return pd.read_csv(input_file, sep='\t')
    else:
        raise ValueError(f"Unsupported input format: {input_ext}")

def write_data(df, output_file):
    output_ext = Path(output_file).suffix.lower()
    if output_ext == '.json':
        with open(output_file, 'w') as f:
            df.fillna("", inplace=True)
            json.dump(df.to_dict(orient='records'), f, indent=4)
    elif output_ext == '.csv':
        df.to_csv(output_file, index=False)
    elif output_ext == '.tsv':
        df.to_csv(output_file, index=False, sep='\t')
    elif output_ext == '.xlsx':
        df.to_excel(output_file, index=False)
    else:
        raise ValueError(f"Unsupported output format: {output_ext}")

def convert_file(input_file, output_file):
    df = read_data(input_file)
    print(len(df))
    write_data(df, output_file)

def main():
    input_file = CURR_DIR / "output.csv"
    output_file = CURR_DIR / "ziprecruiter_jobs_python.json"
    convert_file(input_file, output_file)

if __name__ == "__main__":
    main()