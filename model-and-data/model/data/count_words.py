import json
import csv
from collections import Counter
import re

def clean_text(text):
    # Remove punctuation and convert to lowercase
    text = re.sub(r'[^\w\s]', '', text.lower())
    return text

# Read the JSONL file and process reviews
word_counter = Counter()
with open('data/prepared/common/all_reviews.jsonl', 'r', encoding='utf-8') as file:
    for line in file:
        try:
            review = json.loads(line.strip())
            review_text = review.get('review_text', '')
            # Clean and split text into words
            words = clean_text(review_text).split()
            word_counter.update(words)
        except json.JSONDecodeError:
            continue

# Get the most common words
most_common_words = word_counter.most_common()

# Save to CSV file
with open('data/prepared/word_frequencies.csv', 'w', encoding='utf-8', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Word', 'Frequency'])
    for word, freq in most_common_words:
        writer.writerow([word, freq])

# Save to text file as Python array
with open('data/prepared/word_list.txt', 'w', encoding='utf-8') as txtfile:
    words = [f'"{word}"' for word, _ in most_common_words]
    txtfile.write('arr = [' + ', '.join(words) + ']')