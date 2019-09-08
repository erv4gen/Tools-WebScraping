import  re , time , ipdb , random
import pandas as pd
import multiprocessing as mp
from tqdm import tqdm
from pprint import pprint

from requests import get
from requests.exceptions import RequestException
from contextlib import closing
from user_agent import  generate_user_agent

from bs4 import BeautifulSoup
from readability import Document
from textstat.textstat import textstatistics, easy_word_set, legacy_round
from langdetect import detect


def batch(iterable,n=1):
    l = len(iterable)
    for ndx in range(0,l,n):
        yield iterable[ndx:min(ndx+n,l)]


def extract_text(page):
    
        doc = Document(page)
        test_data = ''.join(extract_text(doc.summary()).splitlines())
        
        return test_data
        soup = BeautifulSoup(page, 'html.parser')
        text = soup.get_text(strip=['\\n','\\u','\\t'])
        return text

def get_web_page(url):
    try:
        url_res = url.strip('.')
        if 'https://' not in url_res:
            url_res= 'https://' + url_res
        #print('Working With URL: ',url_res)
        headers = {'User-Agent': generate_user_agent(device_type="desktop", os=('mac', 'linux'))}
        
        with closing(get(url_res, stream=True, timeout=5, headers=headers)) as resp:
            response_txt = resp.text
        return response_txt
    except:
        return 'Error'

def download_batch(nex_batch):
    res = []
    for url in nex_batch:
        #ipdb.set_trace()
        try:
            listr = get_web_page(url)
            if listr is None or len(listr)<200:
                with lock:
                    print(url, ' - Not a meaningful page, skipping')
                continue
            else:
                print(url, ' - Processing the page')
                
                res.append({'URL': url,'Text':listr})
        except Exception as e:
            print(e)
        finally:
            time.sleep(1)
    
    return pd.DataFrame(res)
        

        
    
def init(l):
    global lock
    lock = l
    
def run_url_download(batch_size=100
                         ,urls_list=None
                        ,path_to_csv=None):
    
    print('Staring program excecution\nDataset size: ', len(urls_list))
        
    random.shuffle(urls_list)
    l = mp.Lock()
    
    pool = mp.Pool(processes=mp.cpu_count(), initializer=init, initargs=(l,))
    
    results = [pool.apply_async(download_batch, args=(url_btch,)) for url_btch in batch(urls_list,batch_size)]
    output = [p.get() for p in results]
    pool.close()
    pool.join()
    res = pd.concat(output)
    res.to_csv(path_to_csv,index=False)
    print('Download finished sucessfully')    
    return res