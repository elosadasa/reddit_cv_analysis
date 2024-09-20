import os
import argparse
import subprocess

def main(dataset_root, output_root, subreddits_file, bot_usernames_file):
    # Create output root directory if it doesn't exist
    os.makedirs(output_root, exist_ok=True)

    # Paths to the comments and submissions directories in the dataset
    comments_dir = os.path.join(dataset_root, 'comments')
    submissions_dir = os.path.join(dataset_root, 'submissions')

    # Paths to the comments and submissions directories in the output
    output_comments_dir = os.path.join(output_root, 'comments')
    output_submissions_dir = os.path.join(output_root, 'submissions')

    # Create output directories if they don't exist
    os.makedirs(output_comments_dir, exist_ok=True)
    os.makedirs(output_submissions_dir, exist_ok=True)

    # Process submissions
    if os.path.exists(submissions_dir):
        submissions_files = [f for f in os.listdir(submissions_dir) if f.endswith('.zst')]
        for filename in submissions_files:
            input_file = os.path.join(submissions_dir, filename)
            # Output will be saved in output_submissions_dir
            print(f"Processing submissions file: {input_file}")
            subprocess.run([
                'python', 'filter_reddit_submissions.py',
                input_file,
                subreddits_file,
                bot_usernames_file,
                '--output_directory', output_submissions_dir
            ], check=True)
    else:
        print(f"Submissions directory not found: {submissions_dir}")

    # Process comments
    if os.path.exists(comments_dir):
        comments_files = [f for f in os.listdir(comments_dir) if f.endswith('.zst')]
        for filename in comments_files:
            input_file = os.path.join(comments_dir, filename)
            # Output will be saved in output_comments_dir
            print(f"Processing comments file: {input_file}")
            subprocess.run([
                'python', 'filter_reddit_comments.py',
                input_file,
                subreddits_file,
                bot_usernames_file,
                '--output_directory', output_comments_dir
            ], check=True)
    else:
        print(f"Comments directory not found: {comments_dir}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process Reddit dataset.')
    parser.add_argument('dataset_root', help='Path to the root folder of the dataset')
    parser.add_argument('output_root', help='Path where the output files will be saved')
    parser.add_argument('subreddits_file', help='Path to the subreddits.txt file')
    parser.add_argument('bot_usernames_file', help='Path to the bot_usernames.txt file')

    args = parser.parse_args()

    main(args.dataset_root, args.output_root, args.subreddits_file, args.bot_usernames_file)