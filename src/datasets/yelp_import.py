import argparse
import json
import os
import asyncio
from datetime import datetime
from tqdm import tqdm
import motor.motor_asyncio
from pymongo import ASCENDING, ReplaceOne
import hashlib

async def insert_documents(collection, documents, unique_key):
    operations = []
    for doc in documents:
        if isinstance(unique_key, list):
            filt = {k: doc[k] for k in unique_key}
        else:
            filt = {unique_key: doc[unique_key]}
        operations.append(
            ReplaceOne(filt, doc, upsert=True)
        )
    if operations:
        try:
            await collection.bulk_write(operations, ordered=False)
        except Exception as e:
            print(f"Error during bulk write: {e}")

async def import_businesses(file_path, db, drop_collection, batch_size=1000):
    collection = db['business']
    if drop_collection:
        await collection.drop()
    else:
        await collection.create_index('business_id', unique=True)

    total_lines = sum(1 for _ in open(file_path, 'r', encoding='utf-8'))
    existing_count = await collection.count_documents({})
    if existing_count >= total_lines:
        print("Businesses collection is up to date. Skipping import.")
        return

    documents = []
    with open(file_path, 'r', encoding='utf-8') as f:
        with tqdm(total=total_lines, desc="Importing Businesses", unit="records") as pbar:
            for line in f:
                try:
                    data = json.loads(line)
                    documents.append(data)
                    if len(documents) >= batch_size:
                        await insert_documents(collection, documents, 'business_id')
                        documents = []
                    pbar.update(1)
                except Exception as e:
                    print(f"Error importing business: {e}")
            if documents:
                await insert_documents(collection, documents, 'business_id')

async def import_reviews(file_path, db, drop_collection, batch_size=1000):
    collection = db['review']
    if drop_collection:
        await collection.drop()
    else:
        await collection.create_index('review_id', unique=True)

    total_lines = sum(1 for _ in open(file_path, 'r', encoding='utf-8'))
    existing_count = await collection.count_documents({})
    if existing_count >= total_lines:
        print("Reviews collection is up to date. Skipping import.")
        return

    documents = []
    with open(file_path, 'r', encoding='utf-8') as f:
        with tqdm(total=total_lines, desc="Importing Reviews", unit="records") as pbar:
            for line in f:
                try:
                    data = json.loads(line)
                    if 'date' in data:
                        try:
                            data['date'] = datetime.strptime(data['date'], '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            data['date'] = datetime.strptime(data['date'], '%Y-%m-%d')
                    documents.append(data)
                    if len(documents) >= batch_size:
                        await insert_documents(collection, documents, 'review_id')
                        documents = []
                    pbar.update(1)
                except Exception as e:
                    print(f"Error importing review: {e}")
            if documents:
                await insert_documents(collection, documents, 'review_id')

async def import_users(file_path, db, drop_collection, batch_size=1000):
    collection = db['user']
    if drop_collection:
        await collection.drop()
    else:
        await collection.create_index('user_id', unique=True)

    total_lines = sum(1 for _ in open(file_path, 'r', encoding='utf-8'))
    existing_count = await collection.count_documents({})
    if existing_count >= total_lines:
        print("Users collection is up to date. Skipping import.")
        return

    documents = []
    with open(file_path, 'r', encoding='utf-8') as f:
        with tqdm(total=total_lines, desc="Importing Users", unit="records") as pbar:
            for line in f:
                try:
                    data = json.loads(line)
                    if 'yelping_since' in data:
                        try:
                            data['yelping_since'] = datetime.strptime(data['yelping_since'], '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            data['yelping_since'] = datetime.strptime(data['yelping_since'], '%Y-%m-%d')
                    documents.append(data)
                    if len(documents) >= batch_size:
                        await insert_documents(collection, documents, 'user_id')
                        documents = []
                    pbar.update(1)
                except Exception as e:
                    print(f"Error importing user: {e}")
            if documents:
                await insert_documents(collection, documents, 'user_id')

async def import_checkins(file_path, db, drop_collection, batch_size=1000):
    collection = db['checkin']
    if drop_collection:
        await collection.drop()
    else:
        await collection.create_index('_id', unique=True)

    total_lines = sum(1 for _ in open(file_path, 'r', encoding='utf-8'))
    existing_count = await collection.count_documents({})
    if existing_count >= total_lines:
        print("Check-ins collection is up to date. Skipping import.")
        return

    documents = []
    with open(file_path, 'r', encoding='utf-8') as f:
        with tqdm(total=total_lines, desc="Importing Check-ins", unit="records") as pbar:
            for line in f:
                try:
                    data = json.loads(line)
                    if 'date' in data and data['date']:
                        date_strings = data['date'].split(', ')
                        dates = []
                        for d in date_strings:
                            d = d.strip()
                            if d:
                                try:
                                    dates.append(datetime.strptime(d, '%Y-%m-%d %H:%M:%S'))
                                except ValueError:
                                    pass
                        data['date'] = dates
                    else:
                        data['date'] = []
                    # Generate _id
                    id_string = f"{data['business_id']}"
                    data['_id'] = hashlib.md5(id_string.encode('utf-8')).hexdigest()
                    documents.append(data)
                    if len(documents) >= batch_size:
                        await insert_documents(collection, documents, '_id')
                        documents = []
                    pbar.update(1)
                except Exception as e:
                    print(f"Error importing check-in: {e}")
            if documents:
                await insert_documents(collection, documents, '_id')

async def import_tips(file_path, db, drop_collection, batch_size=1000):
    collection = db['tip']
    if drop_collection:
        await collection.drop()
    else:
        await collection.create_index('_id', unique=True)

    total_lines = sum(1 for _ in open(file_path, 'r', encoding='utf-8'))
    existing_count = await collection.count_documents({})
    if existing_count >= total_lines:
        print("Tips collection is up to date. Skipping import.")
        return

    documents = []
    with open(file_path, 'r', encoding='utf-8') as f:
        with tqdm(total=total_lines, desc="Importing Tips", unit="records") as pbar:
            for line in f:
                try:
                    data = json.loads(line)
                    if 'date' in data:
                        try:
                            data['date'] = datetime.strptime(data['date'], '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            data['date'] = datetime.strptime(data['date'], '%Y-%m-%d')
                    # Generate _id based on user_id, business_id, date, and text
                    id_string = f"{data['user_id']}_{data['business_id']}_{data['date']}_{data['text']}"
                    data['_id'] = hashlib.md5(id_string.encode('utf-8')).hexdigest()
                    documents.append(data)
                    if len(documents) >= batch_size:
                        await insert_documents(collection, documents, '_id')
                        documents = []
                    pbar.update(1)
                except Exception as e:
                    print(f"Error importing tip: {e}")
            if documents:
                await insert_documents(collection, documents, '_id')

async def main():
    parser = argparse.ArgumentParser(description='Import Yelp dataset into MongoDB.')
    parser.add_argument('folder_path', type=str, help='Path to the folder containing Yelp dataset JSON files')
    parser.add_argument('--mongodb-url', type=str, required=True, help='MongoDB connection URL')
    parser.add_argument('--drop', action='store_true', help='Drop collections before importing')
    args = parser.parse_args()

    client = motor.motor_asyncio.AsyncIOMotorClient(args.mongodb_url)
    db = client['yelp_dataset']

    # List of (filename, import function) tuples
    import_functions = [
        ('yelp_academic_dataset_business.json', import_businesses),
        ('yelp_academic_dataset_review.json', import_reviews),
        ('yelp_academic_dataset_user.json', import_users),
        ('yelp_academic_dataset_checkin.json', import_checkins),
        ('yelp_academic_dataset_tip.json', import_tips)
    ]

    for file_name, import_function in import_functions:
        file_path = os.path.join(args.folder_path, file_name)
        if os.path.exists(file_path):
            try:
                await import_function(file_path, db, args.drop)
            except Exception as e:
                print(f"An error occurred during import of {file_name}: {type(e).__name__}: {e}")
        else:
            print(f"File {file_name} not found in the specified folder.")

    client.close()

if __name__ == '__main__':
    asyncio.run(main())
