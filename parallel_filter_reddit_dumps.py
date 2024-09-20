import os
import argparse
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed

def process_file(script_name, input_file, subreddits_file, bot_usernames_file, output_directory):
    """Function to process a single file using the specified script."""
    try:
        print(f"Processing file: {input_file} with script: {script_name}")
        subprocess.run([
            'python', script_name,
            input_file,
            subreddits_file,
            bot_usernames_file,
            '--output_directory', output_directory
        ], check=True)
        print(f"Finished processing file: {input_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error processing file: {input_file}")
        print(e)
        return False
    return True

def main(dataset_root, output_root, subreddits_file, bot_usernames_file, max_workers=None):
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

    # Prepare tasks for submissions
    tasks = []
    if os.path.exists(submissions_dir):
        submissions_files = [f for f in os.listdir(submissions_dir) if f.endswith('.zst')]
        for filename in submissions_files:
            input_file = os.path.join(submissions_dir, filename)
            tasks.append(('filter_reddit_submissions.py', input_file, output_submissions_dir))
    else:
        print(f"Submissions directory not found: {submissions_dir}")

    # Prepare tasks for comments
    if os.path.exists(comments_dir):
        comments_files = [f for f in os.listdir(comments_dir) if f.endswith('.zst')]
        for filename in comments_files:
            input_file = os.path.join(comments_dir, filename)
            tasks.append(('filter_reddit_comments.py', input_file, output_comments_dir))
    else:
        print(f"Comments directory not found: {comments_dir}")

    # Process files in parallel
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(
                process_file,
                script_name,
                input_file,
                subreddits_file,
                bot_usernames_file,
                output_directory
            ): (script_name, input_file)
            for script_name, input_file, output_directory in tasks
        }

        for future in as_completed(future_to_task):
            script_name, input_file = future_to_task[future]
            try:
                result = future.result()
                if result:
                    print(f"Successfully processed {input_file}")
                else:
                    print(f"Failed to process {input_file}")
            except Exception as exc:
                print(f"{input_file} generated an exception: {exc}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process Reddit dataset in parallel.')
    parser.add_argument('dataset_root', help='Path to the root folder of the dataset')
    parser.add_argument('output_root', help='Path where the output files will be saved')
    parser.add_argument('subreddits_file', help='Path to the subreddits.txt file')
    parser.add_argument('bot_usernames_file', help='Path to the bot_usernames.txt file')
    parser.add_argument('--max_workers', type=int, default=None, help='Maximum number of worker processes to use')

    args = parser.parse_args()

    main(args.dataset_root, args.output_root, args.subreddits_file, args.bot_usernames_file, args.max_workers)
