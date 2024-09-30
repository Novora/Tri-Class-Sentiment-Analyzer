import argparse
import json
import asyncio
from tqdm import tqdm
import motor.motor_asyncio

async def process_reviews(file_path, mongodb_url):
    # Initialize MongoDB client and database
    client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_url)
    db = client.get_default_database()
    collection = db['yelp_reviews_preprocessed']

    # Open the reviews JSON file
    with open(file_path, 'r', encoding='utf-8') as f:
        # Count total lines for progress bar
        total_lines = sum(1 for _ in f)
        f.seek(0)  # Reset file pointer to the beginning

        # Process each line (review) asynchronously
        for line in tqdm(f, total=total_lines, desc="Processing Reviews"):
            review = json.loads(line)
            text = review.get('text', '').strip()
            stars = review.get('stars', 0)

            # Map stars to sentiment
            if stars >= 4:
                sentiment = 1  # Positive
            elif stars == 3:
                sentiment = 0  # Neutral
            else:
                sentiment = -1  # Negative

            # Prepare document to insert
            document = {
                'text': text,
                'sentiment': sentiment
            }

            # Insert into MongoDB
            await collection.insert_one(document)

    # Close MongoDB connection
    client.close()

def main():
    parser = argparse.ArgumentParser(description='Process Yelp reviews and insert into MongoDB.')
    parser.add_argument('file_path', type=str, help='Path to the review.json file')
    parser.add_argument('--mongodb-url', type=str, required=True, help='MongoDB connection URL')
    args = parser.parse_args()

    # Run the asynchronous processing
    asyncio.run(process_reviews(args.file_path, args.mongodb_url))

if __name__ == '__main__':
    main()
