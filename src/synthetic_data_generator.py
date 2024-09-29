import argparse
import openai
import motor.motor_asyncio
import asyncio
import time
from tqdm import tqdm
import logging
from openai.error import OpenAIError
from pymongo.errors import PyMongoError

# Configure logging to write to 'progress.log' in the same directory
logging.basicConfig(
    filename='progress.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# Function to generate synthetic review
async def generate_synthetic_review(sentiment, model_name, max_response_size):
    while True:
        try:
            # Define the chat message with the system and user roles
            messages = [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": f"Generate a review text paragraph with no headings or rating, which is {sentiment} sentiment. Just the text of the review."}
            ]
            # Use the ChatCompletion API
            response = await openai.ChatCompletion.acreate(
                model=model_name,
                messages=messages,
                max_tokens=max_response_size,
                temperature=0.7,
            )
            # Extract the generated text from the response
            return response.choices[0].message.content.strip()
        except OpenAIError as e:
            logging.error(f"OpenAI API error: {str(e)}. Retrying in 10 minutes...")
            await asyncio.sleep(600)  # Wait for 10 minutes before retrying

# Function to insert into MongoDB
async def insert_to_mongodb(collection, document):
    while True:
        try:
            await collection.insert_one(document)
            return
        except PyMongoError as e:
            logging.error(f"MongoDB error: {str(e)}. Retrying in 10 minutes...")
            await asyncio.sleep(600)  # Wait for 10 minutes before retrying

# Function to calculate ETA
def calculate_eta(start_time, progress, total):
    elapsed_time = time.time() - start_time
    remaining = total - progress
    rate = elapsed_time / progress if progress > 0 else 0
    eta = remaining * rate
    eta_h, eta_m = divmod(eta / 3600)
    eta_m = (eta % 3600) // 60
    return int(eta_h), int(eta_m)

# Main function
async def main(args):
    # Initialize the OpenAI client
    openai.api_key = args.openai_api_key
    if args.openai_base_url:
        openai.api_base = args.openai_base_url

    # Initialize MongoDB client
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(args.mongodb_url)
    db = mongo_client["synthetic_reviews"]
    collection = db["reviews"]

    sentiments = ["POSITIVE", "NEUTRAL", "NEGATIVE"]
    sentiment_labels = {"POSITIVE": 1, "NEUTRAL": 0, "NEGATIVE": -1}

    total_samples = args.n_samples_per_class * len(sentiments)
    start_time = time.time()

    # Check existing counts
    existing_counts = {}
    for sentiment in sentiments:
        count = await collection.count_documents({"sentiment": sentiment_labels[sentiment]})
        existing_counts[sentiment] = count

    total_existing = sum(existing_counts.values())
    total_remaining = total_samples - total_existing

    logging.info(f"Existing data points: {existing_counts}")
    logging.info(f"Total data points to generate: {total_remaining}")

    for sentiment in sentiments:
        required_samples = args.n_samples_per_class - existing_counts[sentiment]
        if required_samples <= 0:
            logging.info(f"Already have required samples for {sentiment}")
            continue

        progress_bar = tqdm(total=required_samples, desc=f"Generating {sentiment} reviews")
        generated = 0
        while generated < required_samples:
            # Generate review and insert into MongoDB with retry logic
            review_text = await generate_synthetic_review(sentiment, args.openai_model_name, args.openai_max_response_size)
            document = {"text": review_text, "sentiment": sentiment_labels[sentiment]}
            await insert_to_mongodb(collection, document)
            generated += 1
            progress_bar.update(1)

            # Update progress and log
            progress = total_existing + generated
            eta_h, eta_m = calculate_eta(start_time, progress, total_samples)
            percentage_complete = (progress / total_samples) * 100
            log_message = f"[{progress}/{total_samples}] Synthetic datapoints generated (ETA: {eta_h}h {eta_m}m) ({percentage_complete:.2f}% complete)"
            logging.info(log_message)
            print(log_message)

        progress_bar.close()

    logging.info("Data generation complete!")
    print("Data generation complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic Data Generator")
    parser.add_argument("--n-samples-per-class", type=int, required=True, help="Number of samples per sentiment class")
    parser.add_argument("--mongodb-url", type=str, required=True, help="MongoDB connection URL")
    parser.add_argument("--openai-base-url", type=str, help="OpenAI API base URL")
    parser.add_argument("--openai-model-name", type=str, required=True, help="OpenAI model name to use")
    parser.add_argument("--openai-max-response-size", type=int, default=128, help="Maximum token size for AI model response")
    parser.add_argument("--openai-api-key", type=str, required=True, help="OpenAI API key")
    args = parser.parse_args()

    asyncio.run(main(args))
