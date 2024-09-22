import zstandard as zstd
import json
import pandas as pd
import sys
import os
import io


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
    # Save to Parquet (append mode isn't straightforward for Parquet, so we can save with different file names or use a library that supports appending)
    if not os.path.exists(output_parquet_file):
        df_batch.to_parquet(output_parquet_file, index=False)
    else:
        # Append to Parquet by saving to a new file or using a partitioned Parquet dataset
        df_batch.to_parquet(output_parquet_file, index=False, append=True)


def main(input_file, subreddits_file, bot_file, output_directory=None):
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
    bot_usernames_set = load_list_from_file(bot_file)

    data = []
    BATCH_SIZE = 10000  # Adjust based on available memory
    total_lines = 0
    filtered_not_interest_subreddit = 0
    filtered_bad_lines = 0
    filtered_bots = 0
    filtered_quarantine = 0
    filtered_author_blocked = 0
    filtered_banned = 0
    filtered_removed = 0
    filtered_removed_category = 0
    filtered_over_18 = 0

    subreddit_counts = {}
    filtered_subreddit_counts = {}

    for line in read_zst_file(input_file):
        total_lines += 1
        if not line:
            filtered_bad_lines += 1
            continue
        try:
            obj = json.loads(line)
            subreddit = obj.get('subreddit', '').lower()
            subreddit_counts[subreddit] = subreddit_counts.get(subreddit, 0) + 1

            if subreddit not in subreddits_set:
                filtered_not_interest_subreddit += 1
                continue

            # Increment filtered_subreddit_counts
            filtered_subreddit_counts[subreddit] = filtered_subreddit_counts.get(subreddit, 0) + 1

            author = obj.get('author', '').lower()
            if author in bot_usernames_set or author == '[deleted]':
                filtered_bots += 1
                continue

            # Filter out posts in quarantined subreddits
            if obj.get('quarantine') == True:
                filtered_quarantine += 1
                continue

            # Filter out posts that have been banned by somebody
            if obj.get('banned_by') is not None:
                filtered_banned += 1
                continue

            # Filter out posts that have been removed by somebody
            if obj.get('removed_by') is not None:
                filtered_removed += 1
                continue

            # Filter out posts that have been removed by category
            if obj.get('removed_by_category') is not None:
                filtered_removed_category += 1
                continue

            # Filter out NSFW posts
            if obj.get('over_18') == True:
                filtered_over_18 += 1
                continue

            # Collect relevant fields (excluding the filtering fields)
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
                # Add other fields as needed, excluding the filtering fields
            })

            # If batch size is reached, process and save the batch
            if len(data) >= BATCH_SIZE:
                # Convert list of JSON objects to DataFrame
                df_batch = pd.DataFrame(data)

                # Data type conversions and cleaning
                # (Move this code into a separate function if desired)
                # Convert timestamp fields to datetime
                timestamp_columns = ['created_utc', 'retrieved_on']
                for col in timestamp_columns:
                    df_batch[col] = pd.to_datetime(df_batch[col], unit='s', utc=True, errors='coerce')

                # Convert boolean fields
                boolean_columns = ['author_premium', 'author_is_blocked', 'stickied', 'is_self', 'is_video',
                                   'is_original_content', 'locked', 'saved', 'spoiler', 'media_only', 'can_gild',
                                   'contest_mode', 'no_follow', 'author_patreon_flair', 'pinned', 'hide_score']
                for col in boolean_columns:
                    df_batch[col] = df_batch[col].astype('boolean')

                # Convert numeric fields
                numeric_columns = ['score', 'ups', 'downs', 'num_comments',
                                   'total_awards_received', 'gilded', 'num_crossposts', 'upvote_ratio']
                for col in numeric_columns:
                    df_batch[col] = pd.to_numeric(df_batch[col], errors='coerce')

                # Serialize complex fields to JSON strings
                json_columns = ['gildings', 'all_awardings', 'awarders', 'media', 'media_metadata', 'secure_media']
                for col in json_columns:
                    df_batch[col] = df_batch[col].apply(lambda x: json.dumps(x) if x else 'null')

                # Handle 'distinguished' field
                df_batch['distinguished'] = df_batch['distinguished'].fillna('none')

                # Fill missing strings with empty strings
                string_columns = ['permalink', 'url', 'title', 'selftext', 'author', 'subreddit',
                                  'author_fullname', 'name', 'author_flair_text', 'category']
                for col in string_columns:
                    df_batch[col] = df_batch[col].fillna('')

                # Write the batch to disk
                write_batch_to_disk(df_batch, output_csv_file, output_parquet_file)

                # Clear the data list to free memory
                data.clear()

        except json.JSONDecodeError:
            filtered_bad_lines += 1
            continue  # Skip lines that cannot be parsed

    # Process any remaining data after the loop
    if data:
        df_batch = pd.DataFrame(data)
        # Repeat the data type conversions and cleaning as above
        # ...

        # Write the final batch to disk
        write_batch_to_disk(df_batch, output_csv_file, output_parquet_file)

    # Counts
    total_filtered = (filtered_not_interest_subreddit + filtered_bad_lines + filtered_bots +
                      filtered_quarantine + filtered_author_blocked + filtered_banned +
                      filtered_removed + filtered_removed_category + filtered_over_18)
    total_kept = total_lines - total_filtered

    print(f"Total lines processed: {total_lines}")
    print(f"Total lines kept for analysis: {total_kept}")
    print(f"Total lines filtered out: {total_filtered}")
    print(f"Lines filtered out due to not being in interest subreddits: {filtered_not_interest_subreddit}")
    print(f"Lines filtered out due to bad lines: {filtered_bad_lines}")
    print(f"Lines filtered out due to bots: {filtered_bots}")
    print(f"Lines filtered out due to quarantined subreddits: {filtered_quarantine}")
    print(f"Lines filtered out due to author being blocked: {filtered_author_blocked}")
    print(f"Lines filtered out due to banned posts: {filtered_banned}")
    print(f"Lines filtered out due to removed posts: {filtered_removed}")
    print(f"Lines filtered out due to removed by category: {filtered_removed_category}")
    print(f"Lines filtered out due to NSFW posts: {filtered_over_18}")

    # Save counts to stats_output_file
    with open(stats_output_file, 'w', encoding='utf-8') as stats_file:
        stats_file.write(f"Total lines processed: {total_lines}\n")
        stats_file.write(f"Total lines kept for analysis: {total_kept}\n")
        stats_file.write(f"Total lines filtered out: {total_filtered}\n")
        stats_file.write(
            f"Lines filtered out due to not being in interest subreddits: {filtered_not_interest_subreddit}\n")
        stats_file.write(f"Lines filtered out due to bad lines: {filtered_bad_lines}\n")
        stats_file.write(f"Lines filtered out due to bots: {filtered_bots}\n")
        stats_file.write(f"Lines filtered out due to quarantined subreddits: {filtered_quarantine}\n")
        stats_file.write(f"Lines filtered out due to author being blocked: {filtered_author_blocked}\n")
        stats_file.write(f"Lines filtered out due to banned posts: {filtered_banned}\n")
        stats_file.write(f"Lines filtered out due to removed posts: {filtered_removed}\n")
        stats_file.write(f"Lines filtered out due to removed by category: {filtered_removed_category}\n")
        stats_file.write(f"Lines filtered out due to NSFW posts: {filtered_over_18}\n")
        stats_file.write("\nSubreddit counts (including filtered lines):\n")
        for subreddit, count in subreddit_counts.items():
            stats_file.write(f"{subreddit}: {count}\n")
        stats_file.write("\nSubreddit counts (lines kept for analysis):\n")
        for subreddit, count in filtered_subreddit_counts.items():
            stats_file.write(f"{subreddit}: {count}\n")

    print(f"Data saved to {output_csv_file} and {output_parquet_file}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Process Reddit data.')
    parser.add_argument('input_file', help='Path to the input .zst file')
    parser.add_argument('subreddits_file', help='Path to the subreddits.txt file')
    parser.add_argument('bot_usernames_file', help='Path to the bot_usernames.txt file')
    parser.add_argument('--output_directory', help='Optional output directory for the results')

    args = parser.parse_args()

    main(args.input_file, args.subreddits_file, args.bot_usernames_file, args.output_directory)
