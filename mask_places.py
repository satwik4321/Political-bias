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
print(df['content'].dtypes,df['bias'].dtypes)
df['ner masked data']=None
print(df,df.shape)
print(df['bias'].nunique,df['bias'].unique)
countries=[]
for country in pycountry.countries:
        print(country.name)
        countryl=country.name.split()
        for c in countryl:
            countries.append(c)
def mask(word, names):
    if word in names:
        return names[word], names
    word1 = ''.join(chr(random.randint(65, 90)) for _ in range(3))
    names[word] = word1
    return word1, names

# Cache NLP results to avoid redundant calls
@lru_cache(maxsize=10000)
def get_ner_annotations(word):
    result = nlp.annotate(word, properties={'annotators': 'ner', 'outputFormat': 'json'})
    return json.loads(result)

# Process text and mask words
def mask_text(text,countries):
    print(text)
    names = {}
    result_text = ""
    
    # Split and capitalize first letter of each word
    words = [word[0].upper() + word[1:] for word in text.split()]
    
    for word in words:
        result = get_ner_annotations(word)
        token = result["sentences"][0]['tokens'][0]
        if token['ner'] == "COUNTRY" or token['word'] in countries or token['ner']=="LOCATION" or token['ner']=="SEA" or token['ner']=="MOUNTAIN" or token['ner']=="RIVER" or token['ner']=="CITY":
            word1, names = mask(token['word'], names)
        else:
            word1 = token['word']
        
        result_text += word1 + " "
    print(result_text)    
    return result_text.strip()

# Concurrent processing of the mask_text function
def process_data_concurrently(df, max_workers=1):
    with ThreadPoolExecutor(max_workers=1) as executor:
        future_to_index = {executor.submit(mask_text, row['content'],countries): idx for idx, row in df.iterrows()}
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
df.to_csv(r"C:\Users\sathw\Documents\Masking tasks\Masked output1.csv", index=False)

# Print execution time
print(f"Execution time: {time.time() - start_time} seconds")
print(df['content'].dtypes,df['bias'].dtypes)
print(df)
print(df.shape)