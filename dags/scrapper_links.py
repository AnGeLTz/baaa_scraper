import json
import requests
from bs4 import BeautifulSoup as bs
from tqdm import tqdm
import random
import time

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
]

#Create a list for the URLs
urls = []

# Function to retrieve the number of pages from a URL
def number_pages(url):
     headers = {'User-Agent': random.choice(user_agents)}
     response = requests.get(url,headers=headers)
     if response.status_code == 200:
        soup = bs(response.content,'html.parser')
        last_page = soup.find('a',title='Go to last page')
        return int(last_page['href'][6:]) + 1
     else:
        print(response.status_code)

# Write the list of URLs to a JSON file
def save_to_json(urls):
    with open("urls.json", "w") as file:
        json.dump(urls, file, indent=None)

def run(i):
    headers = {'User-Agent': random.choice(user_agents)}
    #time.sleep(random.uniform(2,6))   
    main = 'https://www.baaa-acro.com/crash-archives?page='+str(i)
    response = requests.get(main,headers=headers)
    if response.status_code == 200:
        soup = bs(response.content,'html.parser')
        # Find all links with class 'red-btn'
        for link in soup.find_all('a', class_ = 'red-btn'):
            # Append the complete URL to the list of URLs
            urls.append('https://www.baaa-acro.com'+link.get('href'))

def main():
    #Get number of the website's pages
    number_of_pages = number_pages("https://www.baaa-acro.com/crash-archives")
    
    # Iterate over the range of page numbers and append links to the urls list
    for i in tqdm(range(0,number_of_pages)):
        run(i)
                    
    # Save the URLs to a JSON file
    save_to_json(urls)

if __name__ == "__main__":
    main()
