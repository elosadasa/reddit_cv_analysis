# filter_reddit_submissions.py

import os
import json
import pandas as pd
import logging
from reddit_utils import read_zst_file, load_list_from_file, write_batch_to_disk

def process_submissions(input_file, subreddits_file, bot_usernames_file, output_directory=None, batch_size=10000):
    """
    Process a single submissions .zst file.
    """
    logging.info(f"Processing submissions file: {input_file}")

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
                'title': obj.get('title'),
                'selftext': obj.get('selftext'),
                'created_utc': obj.get('created_utc'),
                'retrieved_on': obj.get('retrieved_on'),
                'subreddit': obj.get('subreddit'),
                'subreddit_id': obj.get('subreddit_id'),
                'subreddit_type': obj.get('subreddit_type'),
                'score': obj.get('score'),
                'ups': obj.get('ups'),
                'downs': obj.get('downs'),
                'upvote_ratio': obj.get('upvote_ratio'),
                'num_comments': obj.get('num_comments'),
                'total_awards_received': obj.get('total_awards_received'),
                'gilded': obj.get('gilded'),
                'distinguished': obj.get('distinguished'),
                'stickied': obj.get('stickied'),
                'is_self': obj.get('is_self'),
                'is_video': obj.get('is_video'),
                'is_original_content': obj.get('is_original_content'),
                'locked': obj.get('locked'),
                'name': obj.get('name'),
                'saved': obj.get('saved'),
                'spoiler': obj.get('spoiler'),
                'gildings': obj.get('gildings'),
                'all_awardings': obj.get('all_awardings'),
                'awarders': obj.get('awarders'),
                'media_only': obj.get('media_only'),
                'can_gild': obj.get('can_gild'),
                'contest_mode': obj.get('contest_mode'),
                'no_follow': obj.get('no_follow'),
                'author_premium': obj.get('author_premium'),
                'author_patreon_flair': obj.get('author_patreon_flair'),
                'author_flair_text': obj.get('author_flair_text'),
                'num_crossposts': obj.get('num_crossposts'),
                'pinned': obj.get('pinned'),
                'permalink': obj.get('permalink'),
                'url': obj.get('url'),
                'category': obj.get('category'),
                'hide_score': obj.get('hide_score'),
                'media': obj.get('media'),
                'media_metadata': obj.get('media_metadata'),
                'secure_media': obj.get('secure_media'),
                # Add other fields as needed
            })

            if len(data) >= batch_size:
                df_batch = pd.DataFrame(data)
                # Apply data processing steps directly
                logging.debug(f"Number of records before processing: {len(df_batch)}")
                df_batch = process_submissions_data(df_batch)
                logging.debug(f"Number of records after processing: {len(df_batch)}")
                # Write the batch to disk
                write_batch_to_disk(df_batch, output_csv_file, output_parquet_file)
                data.clear()

        except json.JSONDecodeError:
            filtered_counts['bad_lines'] += 1
            continue  # Skip lines that cannot be parsed

    # Process any remaining data
    if data:
        df_batch = pd.DataFrame(data)
        logging.debug(f"Number of records before processing: {len(df_batch)}")
        df_batch = process_submissions_data(df_batch)
        logging.debug(f"Number of records after processing: {len(df_batch)}")
        write_batch_to_disk(df_batch, output_csv_file, output_parquet_file)
        data.clear()

    # Counts
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

def process_submissions_data(df):
    """
    Apply data processing steps specific to submissions.
    """
    # Convert timestamp fields to datetime
    timestamp_columns = ['created_utc', 'retrieved_on']
    for col in timestamp_columns:
        df[col] = pd.to_datetime(df[col], unit='s', utc=True, errors='coerce')

    # Convert boolean fields
    boolean_columns = ['author_premium', 'author_is_blocked', 'stickied', 'is_self', 'is_video',
                       'is_original_content', 'locked', 'saved', 'spoiler', 'media_only',
                       'can_gild', 'contest_mode', 'no_follow', 'author_patreon_flair',
                       'pinned', 'hide_score']
    for col in boolean_columns:
        df[col] = df[col].astype('boolean')

    # Convert numeric fields
    numeric_columns = ['score', 'ups', 'downs', 'num_comments',
                       'total_awards_received', 'gilded', 'num_crossposts', 'upvote_ratio']
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Serialize complex fields to JSON strings
    json_columns = ['gildings', 'all_awardings', 'awarders', 'media',
                    'media_metadata', 'secure_media']
    for col in json_columns:
        df[col] = df[col].apply(lambda x: json.dumps(x) if x else 'null')

    # Handle 'distinguished' field
    df['distinguished'] = df['distinguished'].fillna('none')

    # Fill missing strings with empty strings
    string_columns = ['permalink', 'url', 'title', 'selftext', 'author', 'subreddit',
                      'author_fullname', 'name', 'author_flair_text', 'category']
    for col in string_columns:
        df[col] = df[col].fillna('')

    return df

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Process Reddit submissions data.')
    parser.add_argument('input_file', help='Path to the input .zst file')
    parser.add_argument('subreddits_file', help='Path to the subreddits.txt file')
    parser.add_argument('bot_usernames_file', help='Path to the bot_usernames.txt file')
    parser.add_argument('--output_directory', help='Optional output directory for the results')
    parser.add_argument('--batch_size', type=int, default=10000, help='Number of records to process per batch')

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    process_submissions(
        input_file=args.input_file,
        subreddits_file=args.subreddits_file,
        bot_usernames_file=args.bot_usernames_file,
        output_directory=args.output_directory,
        batch_size=args.batch_size
    )

if __name__ == "__main__":
    main()
