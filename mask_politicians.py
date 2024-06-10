from pycorenlp import StanfordCoreNLP
import json
import random
import csv
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import pycountry
# Initialize timer and NLP server
start_time = time.time()
nlp = StanfordCoreNLP('http://localhost:9000')

# Specify columns to read
columns = ['content', 'bias']

# Read CSV file with specific columns
df = pd.read_csv(r"C:\Users\sathw\Documents\Masking tasks\MediaTest1 1 (4).csv", encoding='latin-1', usecols=columns)
df1 = pd.read_csv(r"C:\Users\sathw\Documents\Masking tasks\politicians1.csv", encoding='latin-1')
politicians=[]
for people in df1['Politicians']:
    for name in people.split():
        politicians.append(name.lower())
print(politicians)
print(df1['Politicians'])
print(df['content'].dtypes,df['bias'].dtypes)
df['ner masked data']=None
print(df,df.shape)
print(df['bias'].nunique,df['bias'].unique)
def mask(word):
    word1 = ''.join(chr(random.randint(65, 90)) for _ in range(3))
    return word1

# Cache NLP results to avoid redundant calls
@lru_cache(maxsize=10000)
def get_ner_annotations(word):
    result = nlp.annotate(word, properties={'annotators': 'ner', 'outputFormat': 'json'})
    return json.loads(result)

# Process text and mask words
def mask_text(text,politicians):
    print(text)
    names = {}
    result_text = ""
    
    # Split and capitalize first letter of each word
    words = [word.lower() for word in text.split()]
    if 'obama' in words:
        print("-----------------------------------------")
    
    for word in words:
        if word in politicians:    
            word1= mask(word)
        else:
            word1=word
        result_text += word1 + " "
    print(result_text)    
    return result_text.strip()

# Concurrent processing of the mask_text function
def process_data_concurrently(df, max_workers=1):
    with ThreadPoolExecutor(max_workers=1) as executor:
        future_to_index = {executor.submit(mask_text, row['content'],politicians): idx for idx, row in df.iterrows()}
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            try:
                df.at[idx, 'masked ner data'] = future.result()
            except Exception as exc:
                print(f"Row {idx} generated an exception: {exc}")

# Apply masking function to the 'content' column
process_data_concurrently(df)

# Select columns to save
output_columns = ['content', 'bias','masked ner data']
df = df[output_columns]

# Save to CSV
df.to_csv(r"C:\Users\sathw\Documents\Masking tasks\Masked_politicians.csv", index=False)

# Print execution time
print(f"Execution time: {time.time() - start_time} seconds")
print(df['content'].dtypes,df['bias'].dtypes)
print(df)
print(df.shape)