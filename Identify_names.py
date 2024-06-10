from pycorenlp import StanfordCoreNLP
import json
import random
import csv
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
# Initialize timer and NLP server
start_time = time.time()
nlp = StanfordCoreNLP('http://localhost:9000')
# Specify columns to read
columns = ['content', 'bias']
# Read CSV file with specific columns
df = pd.read_csv(r"C:\Users\sathw\Documents\Masking tasks\MediaTest1 1 (4).csv", encoding='latin-1', usecols=columns)
df['names']=None
# Cache NLP results to avoid redundant calls
@lru_cache(maxsize=10000)

def check_word(text):
            t1=0
            t2=0
            people=""
            names=[]
            words=[word[0].upper()+word[1:] for word in text.split()]
            for word in words:
                t1=t2
                t2=0
                output = nlp.annotate(word, properties={
                'annotators': 'ner',
                'outputFormat': 'json'
                })
                output=json.loads(output)
                if len(output['sentences'])>0:
                    for word1 in output['sentences'][0]['tokens']:
                        #print(word1)
                        if word1['ner']=="PERSON":
                            t2=1
                        
                            names.append(word)
                            #print(word1['word'],"--name")
                        if t2==1 and t1==1 and names[-1] not in names[:-3] and names[-2] not in names[:-3]:
                            name=""
                            #print(type(names[-2]),type(names[-1]))
                            name=names[-2]+" "+names[-1]
                            if name not in people:
                                #print(name,"--person")
                                people=people+"  "+name
                            t2=0
                            t1=0
            #print(people)            
            return people
            
def process_data_concurrently(df, max_workers=1):
    index=0
    index1=0
    people=[]
    names=[]
    with ThreadPoolExecutor(max_workers=1) as executor:
        future_to_index = {executor.submit(check_word, row['content']): idx for idx, row in df.iterrows()}
        for future in as_completed(future_to_index):
            print(index1)
            try:
                print(future.result())
                for name in future.result().split("  "):
                    print(name)
                    if name not in people:
                        df.at[index,'names']=name
                        index=index+1
                        people.append(name)
                        print(index)
            except Exception as exc:
                print(f"Row {index} generated an exception: {exc}")
            index1+=1
    print(people)
        
process_data_concurrently(df)

# Select columns to save
output_columns = ['names']
df = df[output_columns]

# Save to CSV
df.to_csv(r"C:\Users\sathw\Documents\Masking tasks\names.csv", index=False)

# Print execution time
print(f"Execution time: {time.time() - start_time} seconds")
#print(df['content'].dtypes,df['bias'].dtypes)
print(df)
print(df.shape)