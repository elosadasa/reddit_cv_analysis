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
        dctx = zstd.ZstdDecompressor()
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
    subreddits = load_list_from_file(subreddits_file)
    subreddits_set = set(sub.lower() for sub in subreddits)

    # Load bot usernames from the provided file
    bot_usernames_set = load_list_from_file(bot_file)

    data = []
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

            # Filter out posts whose author is blocked
            if obj.get('author_is_blocked') == True:
                filtered_author_blocked += 1
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
                'created_utc': obj.get('created_utc'),
                'subreddit': obj.get('subreddit'),
                'title': obj.get('title'),
                'selftext': obj.get('selftext'),
                'body': obj.get('body'),
                'score': obj.get('score'),
                'num_comments': obj.get('num_comments'),
                'archived': obj.get('archived'),
                'author_fullname': obj.get('author_fullname'),
                'can_gild': obj.get('can_gild'),
                'can_mod_post': obj.get('can_mod_post'),
                'category': obj.get('category'),
                'created': obj.get('created'),
                'top_awarded_type': obj.get('top_awarded_type'),
                'subreddit_type': obj.get('subreddit_type'),
                'subreddit_subscribers': obj.get('subreddit_subscribers'),
                'subreddit_id': obj.get('subreddit_id'),
                'stickied': obj.get('stickied'),
                'spoiler': obj.get('spoiler'),
                'saved': obj.get('saved'),
                'retrieved_on': obj.get('retrieved_on'),
                'pinned': obj.get('pinned'),
                'num_reports': obj.get('num_reports'),
                'num_crossposts': obj.get('num_crossposts'),
                'name': obj.get('name'),
                'no_follow': obj.get('no_follow'),
                'locked': obj.get('locked'),
                'likes': obj.get('likes'),
                'hide_score': obj.get('hide_score'),
                'hidden': obj.get('hidden'),
                'gilded': obj.get('gilded'),
                'edited': obj.get('edited'),
                'downs': obj.get('downs'),
                'distinguished': obj.get('distinguished'),
                # Add other fields as needed, excluding the filtering fields
            })
        except json.JSONDecodeError:
            filtered_bad_lines += 1
            continue  # Skip lines that cannot be parsed

    # Counts
    total_filtered = (filtered_not_interest_subreddit + filtered_bad_lines + filtered_bots +
                      filtered_quarantine + filtered_author_blocked + filtered_banned +
                      filtered_removed + filtered_removed_category + filtered_over_18)
    total_kept = len(data)

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
        stats_file.write(f"Lines filtered out due to not being in interest subreddits: {filtered_not_interest_subreddit}\n")
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

    if data:
        # Convert list of JSON objects to DataFrame
        df = pd.DataFrame(data)
        # Convert 'created_utc' to datetime
        df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s', utc=True)
        # Convert 'created' to datetime if present
        if 'created' in df.columns:
            df['created'] = pd.to_datetime(df['created'], unit='s', utc=True)
        # Save to CSV
        df.to_csv(output_csv_file, index=False)
        # Save to Parquet
        df.to_parquet(output_parquet_file, index=False)
        print(f"Data saved to {output_csv_file} and {output_parquet_file}")
    else:
        print("No data kept for analysis after filtering.")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Process Reddit data.')
    parser.add_argument('input_file', help='Path to the input .zst file')
    parser.add_argument('subreddits_file', help='Path to the subreddits.txt file')
    parser.add_argument('bot_usernames_file', help='Path to the bot_usernames.txt file')
    parser.add_argument('--output_directory', help='Optional output directory for the results')

    args = parser.parse_args()

    main(args.input_file, args.subreddits_file, args.bot_usernames_file, args.output_directory)
