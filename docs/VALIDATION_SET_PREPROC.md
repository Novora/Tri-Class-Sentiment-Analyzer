# Validation Set Preprocessing Plan
We plan to preprocess the Yelp dataset to get rid of low quality data which falls into the following categories:
1. AI/Computer generated reviews
2. Non English language reviews (we are training an english language based model, so only want english language data).
3. Mislabeled Reviews (The ratings dont match the sentiment of the text)

I will address each of these categories below on how we tackle them:

## AI/Computer generated reviews

1. Only consider reviews before 2nd of January, 2022, when AI was in use a lot more common after that date.
2. analyze the frequency of reviews from each user, if its past a certain threshold, like consistantly making 100 reviews per day, then their reviews can be marked bogus.
3. Look for users that post reviews which are spammy, like promoting a product or service or providing URL's in their review.

