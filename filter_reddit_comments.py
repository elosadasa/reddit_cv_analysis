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
    output_csv_file = os.path.join(output_directory, f'{base_name}_comments.csv')
    output_parquet_file = os.path.join(output_directory, f'{base_name}_comments.parquet')
    stats_output_file = os.path.join(output_directory, f'{base_name}_comments_stats.txt')

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
    filtered_banned = 0
    filtered_crowd_control = 0
    filtered_non_text = 0
    filtered_controversial = 0
    filtered_removed = 0

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

            # Filter: Subreddit of interest
            if subreddit not in subreddits_set:
                filtered_not_interest_subreddit += 1
                continue

            # Increment filtered_subreddit_counts
            filtered_subreddit_counts[subreddit] = filtered_subreddit_counts.get(subreddit, 0) + 1

            author = obj.get('author', '').lower()
            # Filter: Bots
            if author in bot_usernames_set or author == '[deleted]':
                filtered_bots += 1
                continue

            # Filter: Banned comments
            if obj.get('banned_by') is not None:
                filtered_banned += 1
                continue

            # Filter: Collapsed due to crowd control
            if obj.get('collapsed_because_crowd_control'):
                filtered_crowd_control += 1
                continue

            # Filter: Non-textual comments
            if obj.get('comment_type') is not None:
                filtered_non_text += 1
                continue

            # Filter: High controversiality
            if obj.get('controversiality') == 1:
                filtered_controversial += 1
                continue

            # Filter: Removed comments
            if (obj.get('mod_reason_by') is not None or
                    obj.get('mod_reason_title') is not None or
                    obj.get('removal_reason') is not None):
                filtered_removed += 1
                continue

            # Collect relevant fields
            data.append({
                'unrepliable_reason': obj.get('unrepliable_reason'),
                'ups': obj.get('ups'),
                'user_reports': obj.get('user_reports'),
                'subreddit_type': obj.get('subreddit_type'),
                'top_awarded_type': obj.get('top_awarded_type'),
                'replies': obj.get('replies'),
                'report_reasons': obj.get('report_reasons'),
                'retrieved_on': obj.get('retrieved_on'),
                'saved': obj.get('saved'),
                'score': obj.get('score'),
                'score_hidden': obj.get('score_hidden'),
                'send_replies': obj.get('send_replies'),
                'stickied': obj.get('stickied'),
                'subreddit': obj.get('subreddit'),
                'subreddit_id': obj.get('subreddit_id'),
                'total_awards_received': obj.get('total_awards_received'),
                'mod_reports': obj.get('mod_reports'),
                'name': obj.get('name'),
                'created': obj.get('created'),
                'created_utc': obj.get('created_utc'),
                'distinguished': obj.get('distinguished'),
                'downs': obj.get('downs'),
                'edited': obj.get('edited'),
                'gilded': obj.get('gilded'),
                'gildings': obj.get('gildings'),
                'id': obj.get('id'),
                'is_submitter': obj.get('is_submitter'),
                'likes': obj.get('likes'),
                'link_id': obj.get('link_id'),
                'locked': obj.get('locked'),
                'no_follow': obj.get('no_follow'),
                'num_reports': obj.get('num_reports'),
                'parent_id': obj.get('parent_id'),
                'permalink': obj.get('permalink'),
                'collapsed_reason': obj.get('collapsed_reason'),
                'collapsed_reason_code': obj.get('collapsed_reason_code'),
                'body': obj.get('body'),
                'can_gild': obj.get('can_gild'),
                'can_mod_post': obj.get('can_mod_post'),
                'collapsed': obj.get('collapsed'),
                'author_fullname': obj.get('author_fullname'),
                'author_is_blocked': obj.get('author_is_blocked'),
                'author_patreon_flair': obj.get('author_patreon_flair'),
                'author_premium': obj.get('author_premium'),
                'awarders': obj.get('awarders'),
                'banned_at_utc': obj.get('banned_at_utc'),
                'all_awardings': obj.get('all_awardings'),
                'approved_at_utc': obj.get('approved_at_utc'),
                'approved_by': obj.get('approved_by'),
                'archived': obj.get('archived'),
                'associated_award': obj.get('associated_award'),
                'author': obj.get('author'),
            })
        except json.JSONDecodeError:
            filtered_bad_lines += 1
            continue  # Skip lines that cannot be parsed

    # Counts
    total_filtered = (filtered_not_interest_subreddit + filtered_bad_lines + filtered_bots +
                      filtered_banned + filtered_crowd_control + filtered_non_text +
                      filtered_controversial + filtered_removed)
    total_kept = len(data)

    print(f"Total lines processed: {total_lines}")
    print(f"Total lines kept for analysis: {total_kept}")
    print(f"Total lines filtered out: {total_filtered}")
    print(f"Lines filtered out due to not being in interest subreddits: {filtered_not_interest_subreddit}")
    print(f"Lines filtered out due to bad lines: {filtered_bad_lines}")
    print(f"Lines filtered out due to bots: {filtered_bots}")
    print(f"Lines filtered out due to banned comments: {filtered_banned}")
    print(f"Lines filtered out due to crowd control: {filtered_crowd_control}")
    print(f"Lines filtered out due to non-textual comments: {filtered_non_text}")
    print(f"Lines filtered out due to high controversiality: {filtered_controversial}")
    print(f"Lines filtered out due to removed comments: {filtered_removed}")

    # Save counts to stats_output_file
    with open(stats_output_file, 'w', encoding='utf-8') as stats_file:
        stats_file.write(f"Total lines processed: {total_lines}\n")
        stats_file.write(f"Total lines kept for analysis: {total_kept}\n")
        stats_file.write(f"Total lines filtered out: {total_filtered}\n")
        stats_file.write(
            f"Lines filtered out due to not being in interest subreddits: {filtered_not_interest_subreddit}\n")
        stats_file.write(f"Lines filtered out due to bad lines: {filtered_bad_lines}\n")
        stats_file.write(f"Lines filtered out due to bots: {filtered_bots}\n")
        stats_file.write(f"Lines filtered out due to banned comments: {filtered_banned}\n")
        stats_file.write(f"Lines filtered out due to crowd control: {filtered_crowd_control}\n")
        stats_file.write(f"Lines filtered out due to non-textual comments: {filtered_non_text}\n")
        stats_file.write(f"Lines filtered out due to high controversiality: {filtered_controversial}\n")
        stats_file.write(f"Lines filtered out due to removed comments: {filtered_removed}\n")
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
        df['retrieved_on'] = pd.to_datetime(df['retrieved_on'], unit='s', utc=True)
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

    parser = argparse.ArgumentParser(description='Process Reddit comments data.')
    parser.add_argument('input_file', help='Path to the input .zst file')
    parser.add_argument('subreddits_file', help='Path to the subreddits.txt file')
    parser.add_argument('bot_usernames_file', help='Path to the bot_usernames.txt file')
    parser.add_argument('--output_directory', help='Optional output directory for the results')

    args = parser.parse_args()

    main(args.input_file, args.subreddits_file, args.bot_usernames_file, args.output_directory)
