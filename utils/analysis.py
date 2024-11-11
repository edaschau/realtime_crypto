# utils/analysis.py

import sqlite3
from datetime import datetime, timezone
import params
import statistics
import logging

def setup_logging():
    """
    Configures the logging settings.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def remove_outliers(data):
    """
    Removes outliers from a list of numerical values using the IQR method.

    Parameters:
        data (list of float): The list of numerical values.

    Returns:
        list of float: The list with outliers removed.
    """
    if len(data) < 4:
        return data
    q1, q3 = statistics.quantiles(data, n=4)[0], statistics.quantiles(data, n=4)[2]
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    return [x for x in data if lower_bound <= x <= upper_bound]

def connect_database(db_name):
    """
    Establishes a connection to the SQLite database.

    Parameters:
        db_name (str): The name of the database file.

    Returns:
        sqlite3.Connection: The database connection object.
    """
    return sqlite3.connect(db_name)

def ensure_hourly_averages_table(cur, crypto, news_aspects, twitter_aspects):
    """
    Ensures that the 'hourly_averages_<crypto>' table exists with the required dynamic columns.
    If the table does not exist, it creates one. It also adds new columns if new aspects are introduced.

    Parameters:
        cur (sqlite3.Cursor): The database cursor.
        crypto (str): The cryptocurrency name.
        news_aspects (set of str): Set of news aspect names.
        twitter_aspects (set of str): Set of twitter aspect names.
    """
    table_name = f"hourly_averages_{crypto.lower()}"
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    table_exists = cur.fetchone()

    if not table_exists:
        # Create the table with current aspects
        columns = ['crypto_name TEXT', 'utctime TIMESTAMP', 'unixtime INTEGER']
        for aspect in sorted(news_aspects):  # Sorting for consistency
            columns.append(f'"{aspect}_news" REAL')
        for aspect in sorted(twitter_aspects):
            columns.append(f'"{aspect}_twitter" REAL')
        columns.append("PRIMARY KEY (crypto_name, unixtime)")
        create_table_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
        cur.execute(create_table_sql)
        logging.info(f"Created '{table_name}' table with initial columns.")
    else:
        # Retrieve existing columns
        cur.execute(f"PRAGMA table_info({table_name})")
        existing_columns = set(row[1].lower() for row in cur.fetchall())  # Convert to lowercase for uniformity

        # Identify new aspects to add as columns
        new_news_aspects = {aspect for aspect in news_aspects if f"{aspect}_news".lower() not in existing_columns}
        new_twitter_aspects = {aspect for aspect in twitter_aspects if f"{aspect}_twitter".lower() not in existing_columns}

        logging.debug(f"Existing columns in '{table_name}': {existing_columns}")
        logging.debug(f"New news aspects to add: {new_news_aspects}")
        logging.debug(f"New twitter aspects to add: {new_twitter_aspects}")

        # Add new news aspect columns
        for aspect in sorted(new_news_aspects):
            try:
                cur.execute(f'ALTER TABLE {table_name} ADD COLUMN "{aspect}_news" REAL')
                logging.info(f"Added new column '{aspect}_news' to '{table_name}' table.")
            except sqlite3.OperationalError as e:
                logging.error(f"Failed to add column '{aspect}_news' to '{table_name}': {e}")

        # Add new twitter aspect columns
        for aspect in sorted(new_twitter_aspects):
            try:
                cur.execute(f'ALTER TABLE {table_name} ADD COLUMN "{aspect}_twitter" REAL')
                logging.info(f"Added new column '{aspect}_twitter' to '{table_name}' table.")
            except sqlite3.OperationalError as e:
                logging.error(f"Failed to add column '{aspect}_twitter' to '{table_name}': {e}")

def ensure_processed_data_table(cur, crypto, model, data_type, aspects):
    """
    Ensures that the processed data table for a specific crypto, model, and data type exists.
    If the table does not exist, it creates one.

    Parameters:
        cur (sqlite3.Cursor): The database cursor.
        crypto (str): The cryptocurrency name.
        model (dict): The model configuration dictionary.
        data_type (str): Type of data ('news' or 'twitter').
        aspects (set of str): Set of aspect names.
    """
    model_name_formatted = model['name'].replace('/', '_').replace('-', '_').replace('.', '_').lower()
    table_name = f"{crypto.lower()}_{model_name_formatted}_{data_type.lower()}_processed"
    
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    table_exists = cur.fetchone()

    if not table_exists:
        # Create the table with current aspects
        key_column = 'url' if data_type == 'news' else 'tweet_id'
        columns = [f'{key_column} TEXT PRIMARY KEY']  # 'url' for news, 'tweet_id' for twitter
        for aspect in sorted(aspects):
            columns.append(f'"{aspect}" REAL')
        create_table_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
        cur.execute(create_table_sql)
        logging.info(f"Created '{table_name}' table with initial columns.")
    else:
        # Retrieve existing columns
        cur.execute(f"PRAGMA table_info({table_name})")
        existing_columns = set(row[1].lower() for row in cur.fetchall())  # Convert to lowercase for uniformity

        # Identify new aspects to add as columns
        new_aspects = {aspect for aspect in aspects if aspect.lower() not in existing_columns}

        logging.debug(f"Existing columns in '{table_name}': {existing_columns}")
        logging.debug(f"New aspects to add: {new_aspects}")

        # Add new aspect columns
        for aspect in sorted(new_aspects):
            try:
                cur.execute(f'ALTER TABLE {table_name} ADD COLUMN "{aspect}" REAL')
                logging.info(f"Added new column '{aspect}' to '{table_name}' table.")
            except sqlite3.OperationalError as e:
                logging.error(f"Failed to add column '{aspect}' to '{table_name}': {e}")

def get_time_window():
    """
    Defines the time window from the last whole hour to the current time.

    Returns:
        tuple: Start and end times as datetime objects and their corresponding Unix timestamps.
    """
    now = datetime.now(timezone.utc)
    start_time = now.replace(minute=0, second=0, microsecond=0)
    end_time = now
    start_unix = int(start_time.timestamp())
    end_unix = int(end_time.timestamp())
    logging.info(f"Calculating averages from {start_time} (Unix: {start_unix}) to {end_time} (Unix: {end_unix})")
    return start_time, end_time, start_unix, end_unix

def get_common_items(cur, crypto, models, start_unix, end_unix, data_type):
    """
    Retrieves common URLs or tweet IDs across all models for a given crypto within the specified time window.

    Parameters:
        cur (sqlite3.Cursor): The database cursor.
        crypto (str): The cryptocurrency name.
        models (list of dict): List of model configurations.
        start_unix (int): Start of the time window in Unix timestamp.
        end_unix (int): End of the time window in Unix timestamp.
        data_type (str): Type of data to process ('news' or 'twitter').

    Returns:
        set: A set of common URLs or tweet IDs.
    """
    if data_type == 'news':
        table_suffix = '_news'
        join_table = 'articles'
        key_column = 'url'
        time_column = 'articles.datetime_unix'
    elif data_type == 'twitter':
        table_suffix = '_twitter'
        join_table = 'twitter_data'
        key_column = 'tweet_id'
        time_column = 'twitter_data.created_at'
    else:
        raise ValueError("data_type must be either 'news' or 'twitter'.")

    model_tables = [
        f"{crypto}_{model['name'].replace('/', '_').replace('-', '_').replace('.', '_')}{table_suffix}"
        for model in models
    ]

    model_items = []
    for table in model_tables:
        # Check if the table exists
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        if not cur.fetchone():
            logging.warning(f"Table '{table}' does not exist. Skipping.")
            continue

        # Retrieve keys from the table within the time window
        query = f"""
            SELECT {table}.{key_column}
            FROM {table}
            JOIN {join_table} ON {table}.{key_column} = {join_table}.{key_column}
            WHERE {time_column} BETWEEN ? AND ?
        """
        cur.execute(query, (start_unix, end_unix))
        items = set(row[0] for row in cur.fetchall())
        model_items.append(items)
        logging.info(f"Retrieved {len(items)} {key_column}s from table '{table}'.")

    # Identify common items across all models
    if model_items:
        common_items = set.intersection(*model_items)
        logging.info(f"Found {len(common_items)} common {key_column}s across all models for crypto '{crypto}'.")
    else:
        common_items = set()
        logging.info(f"No common {key_column}s found for crypto '{crypto}'.")

    return common_items

def collect_aspect_scores(cur, crypto, models, items, data_type, aspects):
    """
    Collects aspect scores for the common URLs or tweet IDs.

    Parameters:
        cur (sqlite3.Cursor): The database cursor.
        crypto (str): The cryptocurrency name.
        models (list of dict): List of model configurations.
        items (set): Set of common URLs or tweet IDs.
        data_type (str): Type of data ('news' or 'twitter').
        aspects (set of str): Set of aspect names.

    Returns:
        dict: Dictionary mapping each aspect to a list of scores.
    """
    aspect_scores = {aspect: [] for aspect in aspects}

    for item in items:
        for model in models:
            if data_type == 'news':
                table = f"{crypto}_{model['name'].replace('/', '_').replace('-', '_').replace('.', '_')}_news"
            elif data_type == 'twitter':
                table = f"{crypto}_{model['name'].replace('/', '_').replace('-', '_').replace('.', '_')}_twitter"
            else:
                continue

            # Retrieve aspect scores
            columns = ', '.join([f'"{aspect}"' for aspect in aspects])
            query = f"SELECT {columns} FROM \"{table}\" WHERE {'url' if data_type == 'news' else 'tweet_id'} = ?"
            cur.execute(query, (item,))
            row = cur.fetchone()
            if row:
                for i, aspect in enumerate(aspects):
                    score = row[i]
                    if score is not None:
                        aspect_scores[aspect].append(score)

    return aspect_scores

def calculate_average_scores(aspect_scores):
    """
    Removes outliers and calculates the average for each aspect.

    Parameters:
        aspect_scores (dict): Dictionary mapping each aspect to a list of scores.

    Returns:
        dict: Dictionary mapping each aspect to its average score after outlier removal.
    """
    averages = {}
    for aspect, scores in aspect_scores.items():
        clean_scores = remove_outliers(scores)
        if clean_scores:
            avg = sum(clean_scores) / len(clean_scores)
            averages[aspect] = avg
            logging.info(f"Aspect '{aspect}': {len(clean_scores)} scores after outlier removal. Average = {avg:.2f}")
        else:
            logging.info(f"No valid scores for aspect '{aspect}' after outlier removal.")
    return averages

def store_processed_data(cur, crypto, model, data_type, processed_data):
    """
    Inserts processed (outlier-removed) aspect scores into the corresponding processed data table.

    Parameters:
        cur (sqlite3.Cursor): The database cursor.
        crypto (str): The cryptocurrency name.
        model (dict): The model configuration dictionary.
        data_type (str): Type of data ('news' or 'twitter').
        processed_data (dict): Dictionary mapping each aspect to its cleaned scores.
    """
    model_name_formatted = model['name'].replace('/', '_').replace('-', '_').replace('.', '_').lower()
    table_name = f"{crypto.lower()}_{model_name_formatted}_{data_type.lower()}_processed"
    
    for item_id, aspects in processed_data.items():
        # Check if the item already exists
        key_column = 'url' if data_type == 'news' else 'tweet_id'
        cur.execute(f"SELECT 1 FROM {table_name} WHERE {key_column} = ?", (item_id,))
        exists = cur.fetchone()
        
        if exists:
            # Prepare update statement
            set_clause = ', '.join([f'"{aspect}" = ?' for aspect in aspects.keys()])
            update_sql = f"""
                UPDATE {table_name}
                SET {set_clause}
                WHERE {key_column} = ?
            """
            update_values = list(aspects.values()) + [item_id]
            cur.execute(update_sql, update_values)
            logging.info(f"Updated '{table_name}' for {key_column} '{item_id}'.")
        else:
            # Prepare insert statement
            columns = [key_column] + [f'"{aspect}"' for aspect in aspects.keys()]
            placeholders = ', '.join(['?'] * len(columns))
            insert_sql = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES ({placeholders})
            """
            values = [item_id] + list(aspects.values())
            cur.execute(insert_sql, values)
            logging.info(f"Inserted new entry into '{table_name}' for {key_column} '{item_id}'.")

def store_hourly_averages(cur, crypto, utctime_str, unixtime, news_averages, twitter_averages, news_aspects, twitter_aspects):
    """
    Inserts or updates the calculated averages into the 'hourly_averages_<crypto>' table.

    Parameters:
        cur (sqlite3.Cursor): The database cursor.
        crypto (str): The cryptocurrency name.
        utctime_str (str): UTC time as a string.
        unixtime (int): Unix timestamp.
        news_averages (dict): Average scores for news aspects.
        twitter_averages (dict): Average scores for twitter aspects.
        news_aspects (set of str): Set of news aspect names.
        twitter_aspects (set of str): Set of twitter aspect names.
    """
    table_name = f"hourly_averages_{crypto.lower()}"
    
    # Initialize column names and values
    columns = ['crypto_name', 'utctime', 'unixtime']
    values = [crypto, utctime_str, unixtime]

    # Add news aspects
    for aspect in sorted(news_aspects):
        columns.append(f"{aspect}_news")
        values.append(news_averages.get(aspect))

    # Add twitter aspects
    for aspect in sorted(twitter_aspects):
        columns.append(f"{aspect}_twitter")
        values.append(twitter_averages.get(aspect))

    # Check if the row for this crypto and time window already exists
    cur.execute(f"""
        SELECT 1 FROM {table_name}
        WHERE crypto_name = ? AND unixtime = ?
    """, (crypto, unixtime))
    exists = cur.fetchone()

    if exists:
        # Prepare update statement
        set_clause = ', '.join([f'"{col}" = ?' for col in columns[3:]])
        update_sql = f"""
            UPDATE {table_name}
            SET {set_clause}
            WHERE crypto_name = ? AND unixtime = ?
        """
        update_values = values[3:] + [crypto, unixtime]
        cur.execute(update_sql, update_values)
        logging.info(f"Updated '{table_name}' for crypto '{crypto}' at unixtime {unixtime}.")
    else:
        # Prepare insert statement
        placeholders = ', '.join(['?'] * len(values))
        insert_sql = f"""
            INSERT INTO {table_name} ({', '.join(['"'+c+'"' for c in columns])})
            VALUES ({placeholders})
        """
        cur.execute(insert_sql, values)
        logging.info(f"Inserted new entry into '{table_name}' for crypto '{crypto}' at unixtime {unixtime}.")

def process_and_store_processed_data(cur, crypto, models, data_type, items, aspects):
    """
    Processes aspect scores by removing outliers and stores the cleaned data into separate tables.

    Parameters:
        cur (sqlite3.Cursor): The database cursor.
        crypto (str): The cryptocurrency name.
        models (list of dict): List of model configurations.
        data_type (str): Type of data ('news' or 'twitter').
        items (set): Set of common URLs or tweet IDs.
        aspects (set of str): Set of aspect names.
    """
    for model in models:
        logging.info(f"Processing model '{model['name']}' for crypto '{crypto}' and data type '{data_type}'.")
        
        # Ensure the processed data table exists
        ensure_processed_data_table(cur, crypto, model, data_type, aspects)
        
        processed_data = {}

        for item in items:
            # Retrieve aspect scores for this item and model
            if data_type == 'news':
                table = f"{crypto}_{model['name'].replace('/', '_').replace('-', '_').replace('.', '_')}_news"
                key_column = 'url'
            elif data_type == 'twitter':
                table = f"{crypto}_{model['name'].replace('/', '_').replace('-', '_').replace('.', '_')}_twitter"
                key_column = 'tweet_id'
            else:
                continue

            query = f"SELECT {', '.join([f'\"{aspect}\"' for aspect in aspects])} FROM \"{table}\" WHERE {key_column} = ?"
            cur.execute(query, (item,))
            row = cur.fetchone()

            if row:
                aspect_values = {aspect: row[i] for i, aspect in enumerate(aspects) if row[i] is not None}
                if aspect_values:
                    # Remove outliers for each aspect
                    clean_aspects = {}
                    for aspect, value in aspect_values.items():
                        # Assuming outlier removal is handled globally; if not, implement aspect-specific removal here
                        clean_aspects[aspect] = value
                    processed_data[item] = clean_aspects

        # Remove outliers from processed_data
        # Assuming we remove outliers across all items for each aspect
        aspect_scores = {aspect: [data[aspect] for data in processed_data.values() if aspect in data] for aspect in aspects}
        averages = {}
        for aspect, scores in aspect_scores.items():
            clean_scores = remove_outliers(scores)
            if clean_scores:
                avg = sum(clean_scores) / len(clean_scores)
                averages[aspect] = avg
                logging.info(f"Aspect '{aspect}': {len(clean_scores)} scores after outlier removal. Average = {avg:.2f}")
            else:
                logging.info(f"No valid scores for aspect '{aspect}' after outlier removal.")

        # Store the processed data into separate tables
        if processed_data:
            store_processed_data(cur, crypto, model, data_type, processed_data)

def calculate_hourly_averages():
    """
    Main function to calculate hourly averages for both news and twitter data.
    This function orchestrates the entire process by calling other helper functions.
    """
    setup_logging()
    logging.info("Starting hourly averages calculation.")

    # Retrieve configurations from params.py
    DB_NAME = params.get_db_name()
    models = params.get_models()
    cryptos = params.get_crypto_keywords()
    news_prompts = params.get_news_prompts()
    twitter_prompts = params.get_twitter_prompts()

    # Gather unique aspects from prompts
    news_aspects = set(prompt['aspect'].lower().replace(' ', '_') for prompt in news_prompts)
    twitter_aspects = set(prompt['aspect'].lower().replace(' ', '_') for prompt in twitter_prompts)

    # Define the time window: from the last whole hour to the current time
    start_time, end_time, start_unix, end_unix = get_time_window()

    try:
        # Connect to the SQLite database
        conn = connect_database(DB_NAME)
        cur = conn.cursor()

        for crypto, keywords in cryptos.items():
            logging.info(f"Processing crypto: {crypto}")

            # -------------------- Ensure Hourly Averages Table --------------------
            ensure_hourly_averages_table(cur, crypto, news_aspects, twitter_aspects)

            # -------------------- Process News --------------------
            common_urls = get_common_items(cur, crypto, models, start_unix, end_unix, 'news')
            if common_urls:
                # Collect aspect scores for news
                aspect_scores_news = collect_aspect_scores(cur, crypto, models, common_urls, 'news', news_aspects)
                averages_news = calculate_average_scores(aspect_scores_news)

                # Store hourly averages for news
                unixtime = int(start_time.timestamp())
                utctime_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

                store_hourly_averages(
                    cur, crypto, utctime_str, unixtime,
                    averages_news, {},  # No twitter averages here
                    news_aspects, twitter_aspects
                )

                # Additionally, store processed outlier data for each model
                process_and_store_processed_data(cur, crypto, models, 'news', common_urls, news_aspects)
            else:
                logging.info(f"No common URLs to process for crypto '{crypto}'.")

            # -------------------- Process Twitter --------------------
            common_tweet_ids = get_common_items(cur, crypto, models, start_unix, end_unix, 'twitter')
            if common_tweet_ids:
                # Collect aspect scores for twitter
                aspect_scores_twitter = collect_aspect_scores(cur, crypto, models, common_tweet_ids, 'twitter', twitter_aspects)
                averages_twitter = calculate_average_scores(aspect_scores_twitter)

                # Store hourly averages for twitter
                unixtime = int(start_time.timestamp())
                utctime_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

                store_hourly_averages(
                    cur, crypto, utctime_str, unixtime,
                    {}, averages_twitter,  # No news averages here
                    news_aspects, twitter_aspects
                )

                # Additionally, store processed outlier data for each model
                process_and_store_processed_data(cur, crypto, models, 'twitter', common_tweet_ids, twitter_aspects)
            else:
                logging.info(f"No common tweet IDs to process for crypto '{crypto}'.")

            # Commit after processing each crypto
            conn.commit()

    except Exception as e:
        logging.error(f"An error occurred during hourly averages calculation: {e}")
    finally:
        # Close the database connection
        conn.close()
        logging.info("Database connection closed.")
        logging.info("Hourly averages calculation completed.")