import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import gender_guesser.detector as gender
from openai import OpenAI
client = OpenAI(
    api_key="74c1470c2fbd4cf7a78e09d69d5e089b",
    base_url="https://api.aimlapi.com",
)

columns = ['names']
# Read CSV file with specific columns
df = pd.read_csv(r"C:\Users\sathw\Documents\Masking tasks\names.csv", encoding='latin-1', usecols=columns)

df['Gender']=None
def get_content(politician_name):
    user_agent = 'sathwikyamana0@gmail.com'
    headers = {'User-Agent': user_agent}
    url = f"https://en.wikipedia.org/wiki/{politician_name.replace(' ', '_')}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        return None

def predict_gender(name):
    name1=name.split()[0]
    name2=name.split()[1]
    response1 = client.chat.completions.create(
        model="mistralai/Mistral-7B-Instruct-v0.2",
        messages=[
        {
            "role": "system",
            "content": "You are an AI assistant who knows everything.",
        },
        {
            "role": "user",
            "content": "What is the gender of the name {}. Only one word answer pls. And give me the best possible answer based on your search on the internet.It is important that ur answer be either 'Male' or 'Female'".format(name1)
        },
        ],
        )
    
    message = response1.choices[0].message.content
    if message.split()[0][:-1]!="Male" and message.split()[0][:-1]!="Female":
        response2 = client.chat.completions.create(
        model="mistralai/Mistral-7B-Instruct-v0.2",
        messages=[
        {
            "role": "system",
            "content": "You are an AI assistant who knows everything.",
        },
        {
            "role": "user",
            "content": "What is the gender of the name {}. Only one word answer pls. And give me the best possible answer based on your search on the internet.It is important that ur answer be either 'Male' or 'Female'".format(name2)
        },
        ],
        )
        message=response2.choices[0].message.content
    if message.split()[0][:-1]!="Male" and message.split()[0][:-1]!="Female":
        for word in message.split():
            word=word.lower()
            word=word.strip('"\'')
            word=word.strip('.')
            print(word)
            if word=="male" or word=="males" or word=="boys":
                return "Male."
            if word=="female" or word=="females" or word=="girls":
                return "Female."

    return message
#print(f"Assistant: {message.split()[0][:-1]}")

def process_data_concurrently(df, max_workers=1):
    index=0
    index1=0
    with ThreadPoolExecutor(max_workers=1) as executor:
        future_to_index = {executor.submit(predict_gender, row['names']): idx for idx, row in df.iterrows()}
        for future in as_completed(future_to_index):
            try:
                print(future.result())
                if future.result().split()[0][:-1]=="Male" or future.result().split()[0][:-1]=="Female":
                    df.at[index,'Gender']=future.result().split()[0][:-1]
                    #print(df.at[index,'names'],future.result())
            except Exception as exc:
                print(f"Row {index} generated an exception: {exc}")
            index1+=1
            print(index)
            index+=1
        
process_data_concurrently(df)
df.to_csv(r"C:\Users\sathw\Documents\Masking tasks\Gender.csv",index=False)
print(df)