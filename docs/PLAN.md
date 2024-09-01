# Synthetic Data Generation

Use the builtin argparse library to construct the CLI.

Here is the prompt for the AI:
`Generate me a review text paragraph with no headings or rating, which is [SENTIMENT] sentiment. Just the text, of the review.`
[SENTIMENT] can be "NEUTRAL", "POSITIVE" or "NEGATIVE".

The output by the AI model will be labeled with 0, 1 or -1 respectively. Remember to clear the context window after each message. THe `num_predict` (which is the number of max tokens to generate) is `128` by default and will be set with the "--openai-max-response-size" argument. We need equal amount of reviews texts for each category.

We will be using the `motor` package to put the data as its generated, in a MongoDB database.

This is a CLI tool with arguments: "--n-samples-per-class", "--mongodb-url", "--openai-base-url", "--openai-model-name" "--openai-max-response-size" (In tokens).

We will use the `openai` package to send data to the model (without streaming) and we specify the Model name and BASE_URL.

It is important as the data gets added to the database it gives us a status update TOTAL PERSENTAGE, and ETA in this format:
`[x/y] Synthetic datapoints generated (ETA: [x]h[y]m) ([z]% complete)`
