# filter_reddit_comments.py

import os
import json
import pandas as pd
import logging
import csv
from reddit_utils import read_zst_file, load_list_from_file, write_batch_to_disk


def process_comments(input_file, subreddits_file, bot_usernames_file, output_directory=None, batch_size=10000):
    logging.info(f"Processing comments file: {input_file}")

    # Determine the output directory
    if output_directory is None:
        output_directory = os.getcwd()
    else:
        os.makedirs(output_directory, exist_ok=True)

    # Derive output filenames from the input .zst filename
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    output_csv_file = os.path.join(output_directory, f'{base_name}.csv')
    output_parquet_file = os.path.join(output_directory, f'{base_name}.parquet')
    stats_output_file = os.path.join(output_directory, f'{base_name}_stats.txt')

    # Load subreddit names and bot usernames
    subreddits_set = load_list_from_file(subreddits_file)
    bot_usernames_set = load_list_from_file(bot_usernames_file)

    data = []
    total_lines = 0
    total_filtered = 0
    total_processed = 0
    total_written = 0
    filtered_counts = {
        'not_interest_subreddit': 0,
        'bad_lines': 0,
        'bots': 0,
        'quarantine': 0,
        'author_blocked': 0,
        'banned': 0,
        'removed': 0,
        'removed_category': 0,
        'over_18': 0
    }

    subreddit_counts = {}
    filtered_subreddit_counts = {}

    # Initialize header_written flag
    header_written = os.path.exists(output_csv_file)

    for line in read_zst_file(input_file):
        total_lines += 1
        if not line:
            filtered_counts['bad_lines'] += 1
            continue
        try:
            obj = json.loads(line)
            subreddit = obj.get('subreddit', '').lower()
            subreddit_counts[subreddit] = subreddit_counts.get(subreddit, 0) + 1

            if subreddit not in subreddits_set:
                filtered_counts['not_interest_subreddit'] += 1
                continue

            filtered_subreddit_counts[subreddit] = filtered_subreddit_counts.get(subreddit, 0) + 1

            author = obj.get('author', '').lower()
            if author in bot_usernames_set or author == '[deleted]':
                filtered_counts['bots'] += 1
                continue

            # Apply additional filters
            if obj.get('quarantine') == True:
                filtered_counts['quarantine'] += 1
                continue

            if obj.get('banned_by') is not None:
                filtered_counts['banned'] += 1
                continue

            if obj.get('removed_by') is not None:
                filtered_counts['removed'] += 1
                continue

            if obj.get('removed_by_category') is not None:
                filtered_counts['removed_category'] += 1
                continue

            if obj.get('over_18') == True:
                filtered_counts['over_18'] += 1
                continue

            # Collect relevant fields
            data.append({
                'id': obj.get('id'),
                'author': obj.get('author'),
                'author_fullname': obj.get('author_fullname'),
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

            if len(data) >= batch_size:
                df_batch = pd.DataFrame(data)
                rows_before_processing = len(df_batch)

                # Apply data processing steps directly
                df_batch = process_comments_data(df_batch)
                rows_after_processing = len(df_batch)
                rows_dropped = rows_before_processing - rows_after_processing

                # Update counts
                total_processed += rows_after_processing
                total_filtered += rows_dropped

                # Write the batch to disk
                header_written = write_batch_to_disk(df_batch, output_csv_file, output_parquet_file, header_written)
                total_written += len(df_batch)
                data.clear()

                logging.debug(f"Processed batch of size {rows_after_processing}. Total written so far: {total_written}")

        except json.JSONDecodeError as e:
            filtered_counts['bad_lines'] += 1
            logging.error(f"JSON decode error at line {total_lines}: {e}")
            continue  # Skip lines that cannot be parsed

    # Process any remaining data
    if data:
        df_batch = pd.DataFrame(data)
        rows_before_processing = len(df_batch)

        df_batch = process_comments_data(df_batch)
        rows_after_processing = len(df_batch)
        rows_dropped = rows_before_processing - rows_after_processing

        # Update counts
        total_processed += rows_after_processing
        total_filtered += rows_dropped

        header_written = write_batch_to_disk(df_batch, output_csv_file, output_parquet_file, header_written)
        total_written += len(df_batch)
        data.clear()

        logging.debug(f"Processed final batch of size {rows_after_processing}. Total written: {total_written}")

    # Final counts
    logging.info(f"Total lines read: {total_lines}")
    logging.info(f"Total lines processed: {total_processed}")
    logging.info(f"Total lines filtered: {total_filtered}")
    logging.info(f"Total lines written: {total_written}")

    # Save counts to stats_output_file
    with open(stats_output_file, 'w', encoding='utf-8') as stats_file:
        stats_file.write(f"Total lines read: {total_lines}\n")
        stats_file.write(f"Total lines processed: {total_processed}\n")
        stats_file.write(f"Total lines kept for analysis: {total_written}\n")
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


def process_comments_data(df):
    """
    Apply data processing steps specific to comments.
    """
    # Replace newline characters in text fields
    text_columns = ['body']
    for col in text_columns:
        df[col] = df[col].astype(str).str.replace('\n', ' ', regex=False).str.replace('\r', ' ', regex=False)

    # Convert timestamp fields to datetime
    timestamp_columns = ['created_utc', 'retrieved_on', 'approved_at_utc', 'banned_at_utc']
    for col in timestamp_columns:
        df[col] = pd.to_datetime(df[col], unit='s', utc=True, errors='coerce')
        num_missing = df[col].isna().sum()
        if num_missing > 0:
            logging.debug(f"Column '{col}' has {num_missing} missing values after conversion.")

    # Convert boolean fields
    boolean_columns = ['author_premium', 'author_is_blocked', 'stickied', 'score_hidden',
                       'collapsed', 'no_follow', 'can_gild', 'can_mod_post', 'is_submitter',
                       'send_replies', 'archived', 'locked', 'saved', 'author_patreon_flair',
                       'likes']
    for col in boolean_columns:
        df[col] = df[col].astype('boolean')
        num_missing = df[col].isna().sum()
        if num_missing > 0:
            logging.debug(f"Column '{col}' has {num_missing} missing values after conversion.")

    # Convert numeric fields
    numeric_columns = ['score', 'ups', 'downs', 'total_awards_received', 'num_reports', 'gilded']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        num_missing = df[col].isna().sum()
        if num_missing > 0:
            logging.debug(f"Column '{col}' has {num_missing} missing values after conversion.")

    # Serialize complex fields to JSON strings
    json_columns = ['gildings', 'all_awardings', 'awarders', 'mod_reports', 'user_reports', 'report_reasons']
    for col in json_columns:
        df[col] = df[col].apply(lambda x: json.dumps(x) if x else 'null')
        num_missing = df[col].isna().sum()
        if num_missing > 0:
            logging.debug(f"Column '{col}' has {num_missing} missing values after serialization.")

    # Handle 'distinguished' field
    df['distinguished'] = df['distinguished'].fillna('none')

    # Fill missing strings with empty strings
    string_columns = ['permalink', 'body', 'author', 'subreddit', 'author_fullname', 'name',
                      'unrepliable_reason', 'collapsed_reason', 'collapsed_reason_code',
                      'associated_award']
    for col in string_columns:
        df[col] = df[col].fillna('')
        num_missing = df[col].isna().sum()
        if num_missing > 0:
            logging.debug(f"Column '{col}' has {num_missing} missing values after fillna.")

    return df


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Process Reddit comments data.')
    parser.add_argument('input_file', help='Path to the input .zst file')
    parser.add_argument('subreddits_file', help='Path to the subreddits.txt file')
    parser.add_argument('bot_usernames_file', help='Path to the bot_usernames.txt file')
    parser.add_argument('--output_directory', help='Optional output directory for the results')
    parser.add_argument('--batch_size', type=int, default=10000, help='Number of records to process per batch')
    args = parser.parse_args()

    # Configure logging before any logging statements
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    process_comments(
        input_file=args.input_file,
        subreddits_file=args.subreddits_file,
        bot_usernames_file=args.bot_usernames_file,
        output_directory=args.output_directory,
        batch_size=args.batch_size
    )


if __name__ == "__main__":
    main()
