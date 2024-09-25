# filter_reddit_dumps.py

import os
import argparse
import logging

# Import the processing functions
import filter_reddit_submissions
import filter_reddit_comments


def main(dataset_root, output_root, subreddits_file, bot_usernames_file, batch_size=10000):
    # Configure logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    # Create output root directory if it doesn't exist
    os.makedirs(output_root, exist_ok=True)

    # Paths to the comments and submissions directories in the dataset
    comments_dir = os.path.abspath(os.path.join(dataset_root, 'comments'))
    submissions_dir = os.path.abspath(os.path.join(dataset_root, 'submissions'))

    # Paths to the comments and submissions directories in the output
    output_comments_dir = os.path.abspath(os.path.join(output_root, 'comments'))
    output_submissions_dir = os.path.abspath(os.path.join(output_root, 'submissions'))

    # Create output directories if they don't exist
    os.makedirs(output_comments_dir, exist_ok=True)
    os.makedirs(output_submissions_dir, exist_ok=True)

    # Process submissions
    if os.path.exists(submissions_dir):
        submissions_files = [f for f in os.listdir(submissions_dir) if f.endswith('.zst')]
        for filename in submissions_files:
            input_file = os.path.abspath(os.path.join(submissions_dir, filename))
            base_name = os.path.splitext(os.path.basename(input_file))[0]
            output_csv_file = os.path.join(output_submissions_dir, f'{base_name}.csv')
            output_parquet_file = os.path.join(output_submissions_dir, f'{base_name}.parquet')
            stats_output_file = os.path.join(output_submissions_dir, f'{base_name}_stats.txt')
            completion_marker = os.path.join(output_submissions_dir, f'{base_name}_completed.txt')

            # Check if processing is already completed
            if os.path.exists(completion_marker):
                logging.info(f"Skipping processing of submissions file {input_file} as it has been marked completed.")
                continue  # Skip this input file

            # Check if output files exist and are not empty
            output_files = [output_csv_file, output_parquet_file, stats_output_file]
            files_exist = all(os.path.isfile(f) and os.path.getsize(f) > 0 for f in output_files)

            if files_exist:
                logging.info(f"Skipping processing of submissions file {input_file} as output files already exist.")
                # Optionally, you can create the completion marker if it doesn't exist
                if not os.path.exists(completion_marker):
                    with open(completion_marker, 'w') as marker_file:
                        marker_file.write('Processing completed successfully.')
                continue  # Skip this input file

            try:
                filter_reddit_submissions.process_submissions(
                    input_file=input_file,
                    subreddits_file=os.path.abspath(subreddits_file),
                    bot_usernames_file=os.path.abspath(bot_usernames_file),
                    output_directory=output_submissions_dir,
                    batch_size=batch_size
                )
                # Mark processing as completed
                with open(completion_marker, 'w') as marker_file:
                    marker_file.write('Processing completed successfully.')
            except Exception as e:
                logging.error(f"Error processing submissions file {input_file}: {e}")
    else:
        logging.warning(f"Submissions directory not found: {submissions_dir}")

    # Process comments
    if os.path.exists(comments_dir):
        comments_files = [f for f in os.listdir(comments_dir) if f.endswith('.zst')]
        for filename in comments_files:
            input_file = os.path.abspath(os.path.join(comments_dir, filename))
            base_name = os.path.splitext(os.path.basename(input_file))[0]
            output_csv_file = os.path.join(output_comments_dir, f'{base_name}.csv')
            output_parquet_file = os.path.join(output_comments_dir, f'{base_name}.parquet')
            stats_output_file = os.path.join(output_comments_dir, f'{base_name}_stats.txt')
            completion_marker = os.path.join(output_comments_dir, f'{base_name}_completed.txt')

            # Check if processing is already completed
            if os.path.exists(completion_marker):
                logging.info(f"Skipping processing of comments file {input_file} as it has been marked completed.")
                continue  # Skip this input file

            # Check if output files exist and are not empty
            output_files = [output_csv_file, output_parquet_file, stats_output_file]
            files_exist = all(os.path.isfile(f) and os.path.getsize(f) > 0 for f in output_files)

            if files_exist:
                logging.info(f"Skipping processing of comments file {input_file} as output files already exist.")
                # Optionally, you can create the completion marker if it doesn't exist
                if not os.path.exists(completion_marker):
                    with open(completion_marker, 'w') as marker_file:
                        marker_file.write('Processing completed successfully.')
                continue  # Skip this input file

            try:
                filter_reddit_comments.process_comments(
                    input_file=input_file,
                    subreddits_file=os.path.abspath(subreddits_file),
                    bot_usernames_file=os.path.abspath(bot_usernames_file),
                    output_directory=output_comments_dir,
                    batch_size=batch_size
                )
                # Mark processing as completed
                with open(completion_marker, 'w') as marker_file:
                    marker_file.write('Processing completed successfully.')
            except Exception as e:
                logging.error(f"Error processing comments file {input_file}: {e}")
    else:
        logging.warning(f"Comments directory not found: {comments_dir}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process Reddit dataset.')
    parser.add_argument('dataset_root', help='Path to the root folder of the dataset')
    parser.add_argument('output_root', help='Path where the output files will be saved')
    parser.add_argument('subreddits_file', help='Path to the subreddits.txt file')
    parser.add_argument('bot_usernames_file', help='Path to the bot_usernames.txt file')
    parser.add_argument('--batch_size', type=int, default=10000, help='Number of records to process per batch')

    args = parser.parse_args()

    main(
        dataset_root=args.dataset_root,
        output_root=args.output_root,
        subreddits_file=args.subreddits_file,
        bot_usernames_file=args.bot_usernames_file,
        batch_size=args.batch_size
    )
