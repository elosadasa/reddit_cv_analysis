# reddit_utils.py

import zstandard as zstd
import io
import os
import json
import pandas as pd
import logging
import fastparquet
import csv


def read_zst_file(file_path, max_window_size=None):
    """
    Generator function to read lines from a .zst compressed file.
    """
    logging.debug(f"Reading .zst file: {file_path}")
    try:
        with open(file_path, 'rb') as f:
            dctx = zstd.ZstdDecompressor(max_window_size=max_window_size)
            with dctx.stream_reader(f) as reader:
                text_stream = io.TextIOWrapper(reader, encoding='utf-8')
                for line in text_stream:
                    yield line.strip()
    except zstd.ZstdError as e:
        logging.error(f"Zstd decompression error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during decompression: {e}")
        raise



def load_list_from_file(file_path):
    """
    Load items from a text file.
    Each line in the file should contain one item.
    """
    items = set()
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            item = line.strip()
            if item:
                items.add(item.lower())
    logging.debug(f"Loaded {len(items)} items from {file_path}")
    return items


def write_batch_to_disk(df_batch, output_csv_file, output_parquet_file, header_written):
    """
    Write a batch of data to CSV and Parquet files.
    """
    logging.debug(f"Writing batch of size {len(df_batch)} to {output_csv_file} and {output_parquet_file}")

    # Save to CSV in append mode
    df_batch.to_csv(
        output_csv_file,
        mode='a',
        index=False,
        header=not header_written,
        lineterminator='\n',
        quoting=csv.QUOTE_MINIMAL,
        encoding='utf-8'
    )

    # Update the header_written flag
    header_written = True

    # Save to Parquet using fastparquet
    if not os.path.exists(output_parquet_file):
        df_batch.to_parquet(
            output_parquet_file,
            index=False,
            compression='snappy',
            engine='fastparquet'
        )
    else:
        fastparquet.write(
            output_parquet_file,
            df_batch,
            compression='snappy',
            append=True
        )

    return header_written
