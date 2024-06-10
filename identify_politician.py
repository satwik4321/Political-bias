import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
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



def process_data_concurrently(df, max_workers=1):
    index=0
    index1=0
    with ThreadPoolExecutor(max_workers=1) as executor:
        future_to_index = {executor.submit(get_content, row['names']): idx for idx, row in df.iterrows()}
        for future in as_completed(future_to_index):
            try:
                #print(future.result())
                if future.result():
                    print(df.at[index,'names'])
                    soup = BeautifulSoup(future.result(), 'html.parser')
                    tags=soup.find_all('th')
                    flag=0
                    for tag in tags:
                            if tag.text=="Succeded by" or tag.text=="Preceded by":
                                df.at[index1,'Politician']=df.at[index,'names']
                                index1+=1
                                
                else:
                    print(f"Failed to retrieve page for '{df.at[index,'names']}'.")
            except Exception as exc:
                print(f"Row {index} generated an exception: {exc}")
            index1+=1
            print(index)
        
process_data_concurrently(df)
df.to_csv(r"C:\Users\sathw\Documents\Masking tasks\Gender.csv",index=False)
print(df)