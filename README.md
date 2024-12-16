# Reddit Data Filtering and Analysis Toolkit

This project provides a set of Python scripts and utilities for filtering, analyzing, and processing large-scale Reddit data dumps. It allows users to filter submissions and comments based on specific subreddits, remove bot-generated content, and perform efficient parallel processing.

---

## Features

1. **Filter Reddit Submissions**:
   - Filters submissions based on a list of specified subreddits.
   - Removes duplicates and irrelevant data fields.

2. **Filter Reddit Comments**:
   - Filters comments from a Reddit dump.
   - Excludes bot-generated content using a pre-defined list of bot usernames.

3. **Parallel Processing**:
   - Supports parallel filtering of Reddit dumps for increased efficiency on large datasets.

4. **Utility Scripts**:
   - Check for duplicate content within datasets.
   - Shared utilities for common Reddit data processing tasks.

---

## Repository Structure

- `filter_reddit_dumps.py`: Filters Reddit data dumps in parallel for faster processing.
- `filter_reddit_comments.py`: Filters and processes Reddit comments, excluding bot-generated content.
- `filter_reddit_submissions.py`: Filters Reddit submissions based on a list of subreddits.
- `reddit_utils.py`: Utility functions for common Reddit data processing tasks.
- `check_duplicates.py`: Identifies and removes duplicate entries in datasets.
- `subreddits.txt`: A list of target subreddits to filter data from.
- `bot_usernames.txt`: A list of bot usernames to exclude during data filtering.

---

## Setup and Installation

### Prerequisites

- Python 3.8+
- Required Python libraries:
  - `pandas`
  - `argparse`
  - `json`
  - `os`
  - `multiprocessing`