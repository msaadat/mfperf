import requests

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import concurrent.futures


def get(url):
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    return session.get(url)

def threaded_get(urls, max_workers=5):
    ret = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
        futures = {
            executor.submit(get, url):url for url in urls
        }

        for future in concurrent.futures.as_completed(futures):
            url = futures[future]
            try:
                res = future.result()
                ret[url] = res
                
            except Exception as e:
                print(f"Error occurred for item {url}: {e}")
    return ret
        

def post(url, data=None, headers=None):
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    return session.post(url, data=data)