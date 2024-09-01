import argparse
import openai
from openai import AsyncOpenAI
import motor.motor_asyncio
import asyncio
import time
from tqdm import tqdm

# Function to generate synthetic review
async def generate_synthetic_review(sentiment, model_name, client, max_response_size):
    # Define the chat message with the system and user roles
    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": f"Generate me a review text paragraph with no headings or rating, which is {sentiment} sentiment. Just the text, of the review."}
    ]
    # Use the ChatCompletion API
    response = await client.chat.completions.create(
        model=model_name,
        messages=messages,
        max_tokens=max_response_size,
        temperature=0.7,
    )
    # Extract the generated text from the response
    return response.choices[0].message.content.strip()

# Function to insert into MongoDB
async def insert_to_mongodb(mongo_client, database_name, collection_name, document):
    db = mongo_client[database_name]
    collection = db[collection_name]
    await collection.insert_one(document)

# Function to calculate ETA
def calculate_eta(start_time, progress, total):
    elapsed_time = time.time() - start_time
    remaining = total - progress
    rate = elapsed_time / progress if progress > 0 else 0
    eta = remaining * rate
    eta_h, eta_m = divmod(eta / 60, 60)
    return int(eta_h), int(eta_m)

# Main function
async def main(args):
    # Initialize the OpenAI client
    client = AsyncOpenAI(api_key=args.openai_api_key, base_url=args.openai_base_url)
    
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(args.mongodb_url)
    sentiments = ["POSITIVE", "NEUTRAL", "NEGATIVE"]
    sentiment_labels = {"POSITIVE": 1, "NEUTRAL": 0, "NEGATIVE": -1}
    total_samples = args.n_samples_per_class * len(sentiments)
    start_time = time.time()

    for sentiment in sentiments:
        for i in tqdm(range(args.n_samples_per_class), desc=f"Generating {sentiment} reviews"):
            review_text = await generate_synthetic_review(sentiment, args.openai_model_name, client, args.openai_max_response_size)
            document = {"text": review_text, "sentiment": sentiment_labels[sentiment]}
            await insert_to_mongodb(mongo_client, "synthetic_reviews", "reviews", document)
            progress = i + 1 + sentiments.index(sentiment) * args.n_samples_per_class
            eta_h, eta_m = calculate_eta(start_time, progress, total_samples)
            percentage_complete = (progress / total_samples) * 100
            print(f"[{progress}/{total_samples}] Synthetic datapoints generated (ETA: {eta_h}h{eta_m}m) ({percentage_complete:.2f}% complete)")

    print("Data generation complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic Data Generator")
    parser.add_argument("--n-samples-per-class", type=int, required=True, help="Number of samples per sentiment class")
    parser.add_argument("--mongodb-url", type=str, required=True, help="MongoDB connection URL")
    parser.add_argument("--openai-base-url", type=str, required=True, help="OpenAI API base URL")
    parser.add_argument("--openai-model-name", type=str, required=True, help="OpenAI model name to use")
    parser.add_argument("--openai-max-response-size", type=int, default=128, help="Maximum token size for AI model response")
    parser.add_argument("--openai-api-key", type=str, default="test", help="OpenAI API key")
    args = parser.parse_args()
    
    asyncio.run(main(args))