import pandas as pd
import argparse
import logging

def check_duplicates(csv_file, id_column='id'):
    """
    Check for duplicate entries in a CSV file based on a specified column.

    Args:
        csv_file (str): Path to the CSV file.
        id_column (str): The column to check for duplicates. Defaults to 'id'.
    """
    logging.info(f"Reading CSV file: {csv_file}")
    try:
        df = pd.read_csv(csv_file)
        total_rows = len(df)
        logging.info(f"Total rows in CSV: {total_rows}")

        # Check for duplicates
        duplicates = df[df.duplicated(subset=[id_column], keep=False)]
        num_duplicates = len(duplicates)
        num_duplicate_entries = duplicates[id_column].nunique()

        if num_duplicates > 0:
            logging.warning(f"Found {num_duplicates} duplicate rows based on column '{id_column}'.")
            logging.warning(f"Number of unique duplicate entries: {num_duplicate_entries}")

            # Save duplicates to a separate CSV file for inspection
            duplicates_file = csv_file.replace('.csv', '_duplicates.csv')
            duplicates.to_csv(duplicates_file, index=False)
            logging.info(f"Duplicate rows saved to {duplicates_file}")
        else:
            logging.info(f"No duplicate entries found based on column '{id_column}'.")
    except Exception as e:
        logging.error(f"An error occurred while checking for duplicates: {e}")

def main():
    parser = argparse.ArgumentParser(description='Check for duplicate entries in a CSV file.')
    parser.add_argument('csv_file', help='Path to the CSV file to check.')
    parser.add_argument('--id_column', default='id', help='Column name to check for duplicates (default: id).')
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    check_duplicates(args.csv_file, args.id_column)

if __name__ == '__main__':
    main()
