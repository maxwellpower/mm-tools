#!/usr/bin/python3

import mysql.connector
import time
import random
import string

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'mmuser',
    'password': 'mmuserpass',
    'database': 'mattermost'
}

def format_time(seconds):
    """Convert seconds into hours, minutes, and seconds."""
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"

def generate_random_id(length=26):
    """Generate a random ID with the given length."""
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))

def generate_random_message(length=255):
    """Generate a random message with the given length."""
    return ''.join(random.choice(string.ascii_letters + string.digits + string.punctuation + string.whitespace) for _ in range(length))

def fetch_user_channel_ids():
    """Fetch user IDs and their associated channel IDs."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT Id FROM Users LIMIT 30;")
    user_ids = [row[0] for row in cursor.fetchall()]

    user_channel_map = {}
    for user_id in user_ids:
        cursor.execute("SELECT ChannelId FROM ChannelMembers WHERE UserId = %s;", (user_id,))
        channel_ids = [row[0] for row in cursor.fetchall()]
        if channel_ids:  # Only add users with associated channels
            user_channel_map[user_id] = channel_ids

    cursor.close()
    conn.close()

    return user_channel_map

def populate_database(num_posts=12500000):
    """Populate the database with dummy posts."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    user_channel_map = fetch_user_channel_ids()
    user_ids = list(user_channel_map.keys())

    start_time = time.time()  # Start the timer

    print(f"Adding {num_posts} to the database")

    for count in range(1, num_posts + 1):
        try:
            user_id = random.choice(user_ids)
            channel_id = random.choice(user_channel_map[user_id])

            id = generate_random_id()
            createat = int(time.time() * 1000)
            updateat = createat
            message = generate_random_message()

            cursor.execute("INSERT INTO Posts (Id, CreateAt, UpdateAt, DeleteAt, UserId, ChannelId, Message, Props, Filenames, FileIds, HasReactions, EditAt, IsPinned) VALUES (%s, %s, %s, 0, %s, %s, %s, '{}', '[]', '[]', false, 0, false)", (id, createat, updateat, user_id, channel_id, message))

            # Commit and provide feedback every 100,000 posts
            if count % 100000 == 0:
                conn.commit()
                elapsed_time = time.time() - start_time
                print(f"Inserted {count} posts out of {num_posts}. Remaining: {num_posts - count}. Elapsed time: {format_time(elapsed_time)}")

        except Exception as e:
            print(f"Error occurred at count {count}: {e}")
            conn.rollback()

    conn.commit()
    cursor.close()
    conn.close()

    total_time = time.time() - start_time
    print(f"Total time taken to insert {num_posts} posts: {format_time(total_time)}")

def change_collation(charset="utf8mb3", collation="utf8_general_ci"):
    # utf8mb4, utf8mb4_0900_ai_ci / utf8mb3, utf8_general_ci
    """Change the collation of all tables in the database and measure the time taken."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Fetch all table names in the database
    cursor.execute("SHOW TABLES;")
    tables = [table[0] for table in cursor.fetchall()]

    print(f"Tables loaded successfully, updating collation to {charset}, {collation}")

    overall_start_time = time.time()  # Start the timer for the entire operation

    for table in tables:
        table_start_time = time.time()
        try:
            print(f"Updating {table} ...")
            cursor.execute(f"ALTER TABLE {table} CONVERT TO CHARACTER SET {charset} COLLATE {collation};")
            conn.commit()
            table_end_time = time.time() - table_start_time
            print(f"Changed collation for {table}. Time spent: {format_time(table_end_time)}")
        except Exception as e:
            print(f"Error changing collation for {table}: {e}")

    cursor.close()
    conn.close()

    overall_end_time = time.time() - overall_start_time
    print(f"\nTotal time spent changing collation for {len(tables)} tables: {format_time(overall_end_time)}")

def main_populate():
    """Main function to populate the database."""
    print("Database population completed.")
    populate_database()
    print("DONE: Populating database.")

def main_change_collation():
    """Main function to change the collation."""
    print(f"Changing Collation ...")
    change_collation()
    print(f"DONE: Changing collation.")

if __name__ == '__main__':
    choice = input("Choose an action:\n1. Populate Database\n2. Change Collation\nEnter choice (1/2): ")
    if choice == '1':
        main_populate()
    elif choice == '2':
        main_change_collation()
    else:
        print("Invalid choice.")
