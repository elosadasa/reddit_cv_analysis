# filter_reddit_comments.py

import zstandard as zstd
import json
import pandas as pd
import os
import io
import logging

def read_zst_file(file_path):
    """
    Generator function to read lines from a .zst compressed file.
    """
    with open(file_path, 'rb') as f:
        dctx = zstd.ZstdDecompressor(max_window_size=0)
        with dctx.stream_reader(f) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')
            for line in text_stream:
                yield line.strip()

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
    return items

def write_batch_to_disk(df_batch, output_csv_file, output_parquet_file):
    """
    Write a batch of data to CSV and Parquet files.
    """
    # Save to CSV in append mode
    df_batch.to_csv(output_csv_file, mode='a', index=False, header=not os.path.exists(output_csv_file))

    # Save to Parquet
    if not os.path.exists(output_parquet_file):
        df_batch.to_parquet(output_parquet_file, index=False)
    else:
        # Append to Parquet requires handling partitions or using pyarrow's append functionality
        # Here, we concatenate the existing Parquet file with the new batch
        existing_df = pd.read_parquet(output_parquet_file)
        combined_df = pd.concat([existing_df, df_batch], ignore_index=True)
        combined_df.to_parquet(output_parquet_file, index=False)

def process_comments(input_file, subreddits_file, bot_usernames_file, output_directory=None, batch_size=10000):
    """
    Process a single comments .zst file.
    """
    logging.info(f"Processing comments file: {input_file}")

    # Determine the output directory
    if output_directory is None:
        output_directory = os.getcwd()  # Use current working directory
    else:
        # Ensure output directory exists
        os.makedirs(output_directory, exist_ok=True)

    # Derive output filenames from the input .zst filename
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    output_csv_file = os.path.join(output_directory, f'{base_name}.csv')
    output_parquet_file = os.path.join(output_directory, f'{base_name}.parquet')
    stats_output_file = os.path.join(output_directory, f'{base_name}_stats.txt')

    # Load subreddit names from the provided file
    subreddits_set = load_list_from_file(subreddits_file)

    # Load bot usernames from the provided file
    bot_usernames_set = load_list_from_file(bot_usernames_file)

    data = []
    total_lines = 0
    filtered_counts = {
        'not_interest_subreddit': 0,
        'bad_lines': 0,
        'bots': 0,
        'banned': 0,
        'crowd_control': 0,
        'non_text': 0,
        'controversial': 0,
        'removed': 0
    }

    subreddit_counts = {}
    filtered_subreddit_counts = {}

    for line in read_zst_file(input_file):
        total_lines += 1
        if not line:
            filtered_counts['bad_lines'] += 1
            continue
        try:
            obj = json.loads(line)
            subreddit = obj.get('subreddit', '').lower()
            subreddit_counts[subreddit] = subreddit_counts.get(subreddit, 0) + 1

            # Filter: Subreddit of interest
            if subreddit not in subreddits_set:
                filtered_counts['not_interest_subreddit'] += 1
                continue

            # Increment filtered_subreddit_counts
            filtered_subreddit_counts[subreddit] = filtered_subreddit_counts.get(subreddit, 0) + 1

            author = obj.get('author', '').lower()

            # Filter: Bots
            if author in bot_usernames_set or author == '[deleted]':
                filtered_counts['bots'] += 1
                continue

            # Filter: Banned comments
            if obj.get('banned_by') is not None:
                filtered_counts['banned'] += 1
                continue

            # Filter: Collapsed due to crowd control
            if obj.get('collapsed_because_crowd_control'):
                filtered_counts['crowd_control'] += 1
                continue

            # Filter: Non-textual comments
            if obj.get('comment_type') is not None:
                filtered_counts['non_text'] += 1
                continue

            # Filter: High controversiality
            if obj.get('controversiality') == 1:
                filtered_counts['controversial'] += 1
                continue

            # Filter: Removed comments
            if (obj.get('mod_reason_by') is not None or
                obj.get('mod_reason_title') is not None or
                obj.get('removal_reason') is not None):
                filtered_counts['removed'] += 1
                continue

            # Collect relevant fields
            data.append({
                'id': obj.get('id'),
                'author': obj.get('author'),
                'author_fullname': obj.get('author_fullname'),
                'author_premium': obj.get('author_premium'),
                'author_is_blocked': obj.get('author_is_blocked'),
                'body': obj.get('body'),
                'created_utc': obj.get('created_utc'),
                'retrieved_on': obj.get('retrieved_on'),
                'subreddit': obj.get('subreddit'),
                'subreddit_id': obj.get('subreddit_id'),
                'subreddit_type': obj.get('subreddit_type'),
                'score': obj.get('score'),
                'ups': obj.get('ups'),
                'downs': obj.get('downs'),
                'total_awards_received': obj.get('total_awards_received'),
                'gilded': obj.get('gilded'),
                'distinguished': obj.get('distinguished'),
                'stickied': obj.get('stickied'),
                'controversiality': obj.get('controversiality'),
                'permalink': obj.get('permalink'),
                'parent_id': obj.get('parent_id'),
                'link_id': obj.get('link_id'),
                'score_hidden': obj.get('score_hidden'),
                'collapsed': obj.get('collapsed'),
                'collapsed_reason': obj.get('collapsed_reason'),
                'collapsed_reason_code': obj.get('collapsed_reason_code'),
                'no_follow': obj.get('no_follow'),
                'can_gild': obj.get('can_gild'),
                'can_mod_post': obj.get('can_mod_post'),
                'is_submitter': obj.get('is_submitter'),
                'send_replies': obj.get('send_replies'),
                'archived': obj.get('archived'),
                'locked': obj.get('locked'),
                'name': obj.get('name'),
                'saved': obj.get('saved'),
                'gildings': obj.get('gildings'),
                'all_awardings': obj.get('all_awardings'),
                'awarders': obj.get('awarders'),
                'author_patreon_flair': obj.get('author_patreon_flair'),
                'likes': obj.get('likes'),
                'mod_reports': obj.get('mod_reports'),
                'user_reports': obj.get('user_reports'),
                'report_reasons': obj.get('report_reasons'),
                'num_reports': obj.get('num_reports'),
                'banned_at_utc': obj.get('banned_at_utc'),
                'approved_at_utc': obj.get('approved_at_utc'),
                'approved_by': obj.get('approved_by'),
                'associated_award': obj.get('associated_award'),
                'unrepliable_reason': obj.get('unrepliable_reason'),
                # Add other fields as needed
            })

            # If batch size is reached, process and save the batch
            if len(data) >= batch_size:
                process_and_save_batch(
                    data, output_csv_file, output_parquet_file
                )
                data.clear()

        except json.JSONDecodeError:
            filtered_counts['bad_lines'] += 1
            continue  # Skip lines that cannot be parsed

    # Process any remaining data after the loop
    if data:
        process_and_save_batch(
            data, output_csv_file, output_parquet_file
        )
        data.clear()

    # Calculate total filtered lines
    total_filtered = sum(filtered_counts.values())
    total_kept = total_lines - total_filtered

    # Logging the counts
    logging.info(f"Total lines processed: {total_lines}")
    logging.info(f"Total lines kept for analysis: {total_kept}")
    logging.info(f"Total lines filtered out: {total_filtered}")
    for key, value in filtered_counts.items():
        logging.info(f"Lines filtered out due to {key.replace('_', ' ')}: {value}")

    # Save counts to stats_output_file
    with open(stats_output_file, 'w', encoding='utf-8') as stats_file:
        stats_file.write(f"Total lines processed: {total_lines}\n")
        stats_file.write(f"Total lines kept for analysis: {total_kept}\n")
        stats_file.write(f"Total lines filtered out: {total_filtered}\n")
        for key, value in filtered_counts.items():
            stats_file.write(f"Lines filtered out due to {key.replace('_', ' ')}: {value}\n")
        stats_file.write("\nSubreddit counts (including filtered lines):\n")
        for subreddit, count in subreddit_counts.items():
            stats_file.write(f"{subreddit}: {count}\n")
        stats_file.write("\nSubreddit counts (lines kept for analysis):\n")
        for subreddit, count in filtered_subreddit_counts.items():
            stats_file.write(f"{subreddit}: {count}\n")

    logging.info(f"Data saved to {output_csv_file} and {output_parquet_file}")

def process_and_save_batch(data, output_csv_file, output_parquet_file):
    """
    Convert the data list to a DataFrame, process it, and save to disk.
    """
    df_batch = pd.DataFrame(data)

    # Data type conversions and cleaning
    # Convert timestamp fields to datetime
    timestamp_columns = ['created_utc', 'retrieved_on', 'approved_at_utc', 'banned_at_utc']
    for col in timestamp_columns:
        df_batch[col] = pd.to_datetime(df_batch[col], unit='s', utc=True, errors='coerce')

    # Convert boolean fields
    boolean_columns = ['author_premium', 'author_is_blocked', 'stickied', 'score_hidden',
                       'collapsed', 'no_follow', 'can_gild', 'can_mod_post', 'is_submitter',
                       'send_replies', 'archived', 'locked', 'saved', 'author_patreon_flair',
                       'likes']
    for col in boolean_columns:
        df_batch[col] = df_batch[col].astype('boolean')

    # Convert numeric fields
    numeric_columns = ['score', 'ups', 'downs', 'total_awards_received', 'num_reports', 'gilded']
    for col in numeric_columns:
        df_batch[col] = pd.to_numeric(df_batch[col], errors='coerce')

    # Serialize complex fields to JSON strings
    json_columns = ['gildings', 'all_awardings', 'awarders', 'mod_reports', 'user_reports', 'report_reasons']
    for col in json_columns:
        df_batch[col] = df_batch[col].apply(lambda x: json.dumps(x) if x else '[]')

    # Handle 'distinguished' field
    df_batch['distinguished'] = df_batch['distinguished'].fillna('none')

    # Fill missing strings with empty strings
    string_columns = ['permalink', 'body', 'author', 'subreddit', 'author_fullname', 'name',
                      'unrepliable_reason', 'collapsed_reason', 'collapsed_reason_code',
                      'associated_award']
    for col in string_columns:
        df_batch[col] = df_batch[col].fillna('')

    # Write the batch to disk
    write_batch_to_disk(df_batch, output_csv_file, output_parquet_file)
    logging.info(f"Batch written to {output_csv_file} and {output_parquet_file}")

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Process Reddit comments data.')
    parser.add_argument('input_file', help='Path to the input .zst file')
    parser.add_argument('subreddits_file', help='Path to the subreddits.txt file')
    parser.add_argument('bot_usernames_file', help='Path to the bot_usernames.txt file')
    parser.add_argument('--output_directory', help='Optional output directory for the results')
    parser.add_argument('--batch_size', type=int, default=10000, help='Number of records to process per batch')

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    process_comments(
        input_file=args.input_file,
        subreddits_file=args.subreddits_file,
        bot_usernames_file=args.bot_usernames_file,
        output_directory=args.output_directory,
        batch_size=args.batch_size
    )

if __name__ == "__main__":
    main()
