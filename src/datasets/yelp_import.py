import argparse
import json
import os
import asyncio
from datetime import datetime
from tqdm import tqdm
import motor.motor_asyncio

async def import_businesses(file_path, db):
    collection = db['business']
    await collection.drop()  # Drop collection if it exists
    with open(file_path, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f)
        f.seek(0)
        with tqdm(total=total_lines, desc="Importing Businesses", unit="records") as pbar:
            for line in f:
                data = json.loads(line)
                # No special date fields to process
                await collection.insert_one(data)
                pbar.update(1)

async def import_reviews(file_path, db):
    collection = db['review']
    await collection.drop()
    with open(file_path, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f)
        f.seek(0)
        with tqdm(total=total_lines, desc="Importing Reviews", unit="records") as pbar:
            for line in f:
                data = json.loads(line)
                # Parse date field with time
                if 'date' in data:
                    try:
                        data['date'] = datetime.strptime(data['date'], '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        # Handle date strings without time
                        data['date'] = datetime.strptime(data['date'], '%Y-%m-%d')
                await collection.insert_one(data)
                pbar.update(1)

async def import_users(file_path, db):
    collection = db['user']
    await collection.drop()
    with open(file_path, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f)
        f.seek(0)
        with tqdm(total=total_lines, desc="Importing Users", unit="records") as pbar:
            for line in f:
                data = json.loads(line)
                # Parse yelping_since date field
                if 'yelping_since' in data:
                    data['yelping_since'] = datetime.strptime(data['yelping_since'], '%Y-%m-%d')
                await collection.insert_one(data)
                pbar.update(1)

async def import_checkins(file_path, db):
    collection = db['checkin']
    await collection.drop()
    with open(file_path, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f)
        f.seek(0)
        with tqdm(total=total_lines, desc="Importing Check-ins", unit="records") as pbar:
            for line in f:
                data = json.loads(line)
                # Process date field: comma-separated list of timestamps
                if 'date' in data and data['date']:
                    date_strings = data['date'].split(', ')
                    dates = []
                    for d in date_strings:
                        d = d.strip()
                        if d:
                            try:
                                dates.append(datetime.strptime(d, '%Y-%m-%d %H:%M:%S'))
                            except ValueError:
                                pass  # Skip invalid date formats
                    data['date'] = dates
                else:
                    data['date'] = []
                await collection.insert_one(data)
                pbar.update(1)

async def import_tips(file_path, db):
    collection = db['tip']
    await collection.drop()
    with open(file_path, 'r', encoding='utf-8') as f:
        total_lines = sum(1 for _ in f)
        f.seek(0)
        with tqdm(total=total_lines, desc="Importing Tips", unit="records") as pbar:
            for line in f:
                data = json.loads(line)
                # Parse date field with time
                if 'date' in data:
                    try:
                        data['date'] = datetime.strptime(data['date'], '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        # Handle date strings without time
                        data['date'] = datetime.strptime(data['date'], '%Y-%m-%d')
                await collection.insert_one(data)
                pbar.update(1)

async def main():
    parser = argparse.ArgumentParser(description='Import Yelp dataset into MongoDB.')
    parser.add_argument('folder_path', type=str, help='Path to the folder containing Yelp dataset JSON files')
    parser.add_argument('--mongodb-url', type=str, required=True, help='MongoDB connection URL')
    args = parser.parse_args()

    client = motor.motor_asyncio.AsyncIOMotorClient(args.mongodb_url)
    db = client['yelp_dataset']

    tasks = []

    # Map file names to their import functions
    import_functions = {
        'yelp_academic_dataset_business.json': import_businesses,
        'yelp_academic_dataset_review.json': import_reviews,
        'yelp_academic_dataset_user.json': import_users,
        'yelp_academic_dataset_checkin.json': import_checkins,
        'yelp_academic_dataset_tip.json': import_tips
    }

    for file_name, import_function in import_functions.items():
        file_path = os.path.join(args.folder_path, file_name)
        if os.path.exists(file_path):
            tasks.append(import_function(file_path, db))
        else:
            print(f"File {file_name} not found in the specified folder.")

    # Run import tasks sequentially to avoid overwhelming the system
    for task in tasks:
        try:
            await task
        except Exception as e:
            print(f"An error occurred during import: {e}")

    client.close()

if __name__ == '__main__':
    asyncio.run(main())
