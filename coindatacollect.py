import requests
from requests import RequestException
import pandas as pd
import json
import time
import csv
import random
import os.path
import copy
from datetime import datetime
from datetime import date
from bs4 import BeautifulSoup
from fake_useragent import UserAgent, FakeUserAgentError


ua = UserAgent(verify_ssl=False)
USER_AGENTS = ['Mozilla/5.0 (X11; CrOS i686 4319.74.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.57 Safari/537.36',
               'Mozilla/5.0 (Windows NT 6.1; rv:14.0) Gecko/20100101 Firefox/18.0.1',
               'Opera/12.0(Windows NT 5.2;U;en)Presto/22.9.168 Version/12.00',
               'Mozilla/5.0 (Windows; U; Windows NT 6.1; ko-KR) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27']

FIELDSNAME = ['Date','Name','Symbol', 'Rank', 'Price_USD', 'Price_BTC', 'Market_Cap_USD', 'Id', 
                'Data:Twitter',
                'Data:Twitter:followers',
                'Data:Twitter:lists',
                'Data:Twitter:favourites',
                'Data:Twitter:statuses',
                'Data:Twitter:Points',
                'Data:Reddit',
                'Data:Reddit:subscribers',
                'Data:Reddit:active_users',
                'Data:Reddit:community_creation',
                'Data:Reddit:posts_per_hour',
                'Data:Reddit:posts_per_day',
                'Data:Reddit:comments_per_hour',
                'Data:Reddit:comments_per_day',
                'Data:Facebook',
                'Data:Facebook:likes',
                'Data:Facebook:talking_about about this page',
                'Data:Facebook:Points',
                'Data:CodeRepository',
                'Data:CodeRepository:List[]:stars',
                'Data:CodeRepository:List[]:forks forks',
                'Data:CodeRepository:List[]:open_total_issues requests and bugs',
                'Data:CodeRepository:List[]:subscribers',
                'Data:CodeRepository:List[]:open_pull_issues',
                'Data:CodeRepository:List[]:closed_pull_issues',
                'Data:CodeRepository:List[]:closed_total_issues',
                'Data:CodeRepository:List[]:open_issues',
                'Data:CodeRepository:List[]:closed_issues', 
                'Data:CodeRepository:Points',
                'Coingecko:Closed Issues',
                'Coingecko:Contributors',
                'Coingecko:Forks',
                'Coingecko:Merged Pull Requests',
                'Coingecko:Stars',
                'Coingecko:Total Issues',
                'Coingecko:Total new commits in the last 4 weeks',
                'Coingecko:Watchers'
            ]

OUTPUTFILE = datetime.fromtimestamp(time.time()).strftime('%c') + '.csv'

def get_coinmarketcap_data ():
    '''Get data of coins from coinmarketcap.com'''
    url = 'https://api.coinmarketcap.com/v1/ticker/'
    params = {'start':0,'limit':TOP}
    try:
        headers = {'User-Agent':ua.random}
    except FakeUserAgentError:
        headers = {'User-Agent': USER_AGENTS[random.randint(0,3)]}   
    try: 
        response = requests.get(url, headers=headers, params=params)
    except Exception:
        print('There is not data, because coinmarketcap.com received exception')
        return []
    
    if response.status_code == 200:
        content = json.loads(response.content.decode('utf-8'))
        coin_data = []
        for coin in content:
            coin_info = {}
            coin_info ['Name'] = coin['name']
            coin_info ['Symbol'] = coin['symbol']
            
            try:
                coin_info ['Rank'] = coin['rank']
            except TypeError:
                coin_info ['Rank'] = None
            
            try:
                coin_info ['Price_USD'] = float(coin['price_usd'])
            except TypeError:
                coin_info ['Price_USD'] = None
            
            try:
                coin_info ['Price_BTC'] = float(coin['price_btc'])
            except TypeError:
                coin_info ['Price_BTC'] = None
            
            try:
                coin_info ['Market_Cap_USD'] = float(coin['market_cap_usd'])
            except TypeError:
                coin_info ['Market_Cap_USD'] = None   
            
            coin_data.append(coin_info)
        
    else:
        print('There is not data, because coinmarketcap.com answer ' + str(response.status_code))

    return coin_data

def get_cryptocompare_coinlist ():
    '''Get list of coins from cryptocompare.com'''
    url = 'https://min-api.cryptocompare.com/data/all/coinlist'
    try:
        headers = {'User-Agent':ua.random}
    except FakeUserAgentError:
        headers = {'User-Agent': USER_AGENTS[random.randint(0,3)]}
    try: 
        response = requests.get(url, headers=headers)
    except Exception:
        print('There is not data, because cryptocompare.com received exception')
        return []
    
    if response.status_code == 200:
        content = json.loads(response.content.decode('utf-8'))
        if content['Response'] == 'Success':
            return content['Data']
        else:
            print (content['Response'])
            return []
    else:
        print('There is not data, because cryptocompare.com answer ' + str(response.status_code))
        return []
    
def get_cryptocompare_id (coin_data, cc_coinlist):
    '''Add cryptocompare ids to coin_data'''
    for i in range(len(coin_data)):
        try:
            coin_data[i]['Id'] = cc_coinlist[coin_data[i]['Symbol']]['Id']
        except KeyError:
            coin_data[i]['Id'] = ''
    return coin_data

def sum_values_inlist(cr_list):   
    '''Sum values of dict in list of CodeRepository cryptocompare.com'''
    keys = ['subscribers',
            'open_issues',
            'forks',
            'closed_pull_issues',
            'stars',
            'open_total_issues',
            'closed_issues',
            'closed_total_issues',
            'open_pull_issues'
           ]
    result_dict = {}
    for key in keys:
        for item_dict in cr_list:
            try:
                res_value = int(result_dict[key])
            except (KeyError, ValueError):
                res_value = 0
            try:
                value = int(item_dict[key])
            except (KeyError, ValueError):
                continue
            result_dict[key] = res_value + value
    return result_dict

def get_cryptocompare_socialstats (coin_id):
    '''Get socialstats of coin by id from cryptocompare.com'''
    url = 'https://www.cryptocompare.com/api/data/socialstats/'
    params = {'id':coin_id}
    try:
        headers = {'User-Agent':ua.random}
    except FakeUserAgentError:
        headers = {'User-Agent': USER_AGENTS[random.randint(0,3)]}

    try: 
        response = requests.get(url, headers=headers, params=params)
    except Exception:
        print('There is not data, because cryptocompare.com received exception')
        return {}
    
    if response.status_code == 200:
        content = json.loads(response.content.decode('utf-8'))
        if content['Response'] == 'Success':
            data =  content['Data']
            coin_socialstats = {}
            #Twitter
            try:
                coin_socialstats['Data:Twitter:followers'] = int(data['Twitter']['followers'])
            except (KeyError, ValueError):
                pass
            try:
                coin_socialstats['Data:Twitter:lists'] = int(data['Twitter']['lists'])
            except (KeyError, ValueError):
                pass
            try:
                coin_socialstats['Data:Twitter:favourites'] = int(data['Twitter']['favourites'])
            except (KeyError, ValueError):
                pass
            try:
                coin_socialstats['Data:Twitter:statuses'] = int(data['Twitter']['statuses'])
            except (KeyError, ValueError):
                pass
            try:
                coin_socialstats['Data:Twitter:Points'] = int(data['Twitter']['Points'])
            except (KeyError, ValueError):
                pass
                
            #Reddit
            try:
                coin_socialstats['Data:Reddit:subscribers'] = int(data['Reddit']['subscribers'])
            except (KeyError, ValueError):
                pass
            try:
                coin_socialstats['Data:Reddit:active_users'] = int(data['Reddit']['active_users'])
            except (KeyError, ValueError):
                pass
            try:
                coin_socialstats['Data:Reddit:community_creation'] = datetime.fromtimestamp(int(data['Reddit']['community_creation']))
            except (KeyError, ValueError):
                pass
            try:
                coin_socialstats['Data:Reddit:posts_per_hour'] = float(data['Reddit']['posts_per_hour'])
            except (KeyError, ValueError):
                pass
            try:
                coin_socialstats['Data:Reddit:posts_per_day'] = float(data['Reddit']['posts_per_day'])
            except (KeyError, ValueError):
                pass
            try:
                coin_socialstats['Data:Reddit:comments_per_hour'] = float(data['Reddit']['comments_per_hour'])
            except (KeyError, ValueError):
                pass
            try:
                coin_socialstats['Data:Reddit:comments_per_day'] = float(data['Reddit']['comments_per_day'])
            except (KeyError, ValueError):
                pass
            
            #Facebook
            try:
                coin_socialstats['Data:Facebook:likes'] = int(data['Facebook']['likes'])
            except (KeyError, ValueError):
                pass
            try:
                coin_socialstats['Data:Facebook:talking_about about this page'] = int(data['Facebook']['talking_about'])
            except (KeyError, ValueError):
                pass
            try:
                coin_socialstats['Data:Facebook:Points'] = int(data['Facebook']['Points'])
            except (KeyError, ValueError):
                pass
        
            #CodeRepository
            try:
                cr_list = sum_values_inlist(data['CodeRepository']['List'])
            except (KeyError, IndexError, ValueError):
                cr_list = {}
            
            try:
                coin_socialstats['Data:CodeRepository:List[]:stars'] = int(cr_list['stars'])
            except (KeyError, IndexError, ValueError):
                pass
            try:
                coin_socialstats['Data:CodeRepository:List[]:forks forks'] = int(cr_list['forks'])
            except (KeyError, IndexError, ValueError):
                pass
            try:
                coin_socialstats['Data:CodeRepository:List[]:open_total_issues requests and bugs'] = int(cr_list['open_total_issues'])       
            except (KeyError, IndexError, ValueError):
                pass
            try:
                coin_socialstats['Data:CodeRepository:List[]:subscribers'] = int(cr_list['subscribers'])
            except (KeyError, IndexError, ValueError):
                pass
            try:
                coin_socialstats['Data:CodeRepository:List[]:open_pull_issues'] = int(cr_list['open_pull_issues'])
            except (KeyError, IndexError, ValueError):
                pass
            try:
                coin_socialstats['Data:CodeRepository:List[]:closed_pull_issues'] = int(cr_list['closed_pull_issues'])
            except (KeyError, IndexError, ValueError):
                pass
            try:
                coin_socialstats['Data:CodeRepository:List[]:closed_total_issues'] = int(cr_list['closed_total_issues'])
            except (KeyError, IndexError, ValueError):
                pass
            try:
                coin_socialstats['Data:CodeRepository:List[]:open_issues'] = int(cr_list['open_issues'])
            except (KeyError, IndexError, ValueError):
                pass
            try:
                coin_socialstats['Data:CodeRepository:List[]:closed_issues'] = int(cr_list['closed_issues'])
            except (KeyError, IndexError, ValueError):
                pass
            try:
                coin_socialstats['Data:CodeRepository:Points'] = int(data['CodeRepository']['Points'])
            except (KeyError, IndexError, ValueError):
                pass                  
                
            return coin_socialstats
        else:
            print (content['Response'])
            return {}
    else:
        print('There is not data, because cryptocompare.com answer ' + str(response.status_code))
        return {}

def get_coingecko_lastnumber_pagination ():
    url = 'https://www.coingecko.com/en'
    
    try:
        headers = {'User-Agent':ua.random}
    except FakeUserAgentError:
        headers = {'User-Agent': USER_AGENTS[random.randint(0,3)]}
    try:
        response = requests.get(url, headers=headers)
    except Exception:
        return None
    if response.status_code == 200:
        content = response.content.decode('utf-8', 'ignore')
        soup = BeautifulSoup(content, 'lxml')
        try:
            lis = soup.find('ul', {'class':'pagination'}).find_all('li')
            last_page_url = lis[-1].find('a').attrs['href']
            lastnumber_pagination = int(last_page_url.split('=')[-1])
        except (KeyError, AttributeError, ValueError, IndexError):
            return None
        else:
            return lastnumber_pagination
    else:
        print('There is not data, because www.coingecko.com answer ' + str(response.status_code))
        return None  

def get_coingecko_content(pagenum):
    url = 'https://www.coingecko.com/en'
    params = {'page':pagenum}
    try:
        headers = {'User-Agent':ua.random}
    except FakeUserAgentError:
        headers = {'User-Agent': USER_AGENTS[random.randint(0,3)]}
    try:
        response = requests.get(url, headers=headers, params=params)
    except Exception:
        return None
    if response.status_code == 200:
        content = response.content.decode('utf-8', 'ignore')
        return content
    else:
        print('There is not data, because www.coingecko.com answer ' + str(response.status_code))
        return None     

def get_coingecko_data_content (content):
    soup = BeautifulSoup(content, 'lxml')
    try:
        trs = soup.find('table', {'id':'gecko-table'}).find('tbody').find_all('tr')
    except (KeyError, AttributeError):
        return None
    coingecko_data_page = []
    for tr in trs:
        row_coin_data = {}
        try:
            symbol = tr.find('td', {'class':'coin-name'}).find('span',{'class':'coin-content-symbol'}).text.strip()
        except (KeyError, AttributeError):
            continue
        else:
            row_coin_data['Coingecko:Symbol'] = symbol
            try:
                tooltip_html = tr.find('td', {'class':'td-developer_score dev'}).find('div').attrs['title']
            except (KeyError, AttributeError):
                pass
            else:
                dev_data = get_tooltip_dev_data (tooltip_html)
                row_coin_data.update(dev_data)
            coingecko_data_page.append(row_coin_data)
    return coingecko_data_page

def get_tooltip_dev_data (tooltip_html):
    tooltip_soup = BeautifulSoup(tooltip_html, 'lxml')
    tooltip_data = {}
    try:
        trs = tooltip_soup.find_all('tr')
    except (KeyError, AttributeError):
        return tooltip_data
    for tr in trs:
        tds = tr.find_all('td')
        try:
            key = 'Coingecko:' + str(tds[0].text.strip())
        except:
            pass
        else:
            try:
                value = int(tds[1].text.strip())
            except (ValueError, KeyError, AttributeError):
                value = 0
            tooltip_data[key] = value
    return tooltip_data   

def get_coingecko_data():
    '''get data of all coin from coingecko.com'''
    coingecko_data = []
    nums_pages = get_coingecko_lastnumber_pagination ()
    if nums_pages is not None:  
        for pagenum in range(1, nums_pages + 1):
            print('CoinGecko.com: Loading from {} page of {}...'.format(pagenum, nums_pages))
            content = get_coingecko_content(pagenum)
            if content is not None:
                coingecko_data_page = get_coingecko_data_content (content)
                if coingecko_data_page is not None:
                    coingecko_data += coingecko_data_page
                    #print('Data from {} page is loaded.'.format(pagenum))
            time.sleep(5)
    return coingecko_data 

def get_coingecko_data_fromlist (symbol, coingecko_data):
    '''Get only data from all data by symbol'''
    coin_data = copy.deepcopy(coingecko_data)
    for coin in coin_data:
        if symbol == coin['Coingecko:Symbol']:
            coin.pop('Coingecko:Symbol')
            return coin
    return {}

def save_to_file(coin_data):
    '''Save coin_data to CSV'''
    if os.path.isfile(OUTPUTFILE):
        mode = 'a'
    else:
        mode = 'w'
    with open(OUTPUTFILE, mode = mode, newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, delimiter = ',', fieldnames = FIELDSNAME)
        if mode == 'w':
            dict_writer.writeheader()
        for coin in coin_data:
            dict_writer.writerow(coin)

def save_to_dailyfile(coin_data):
    '''Save coin_data to daily CSV'''
    folder = 'daily'
    filename = today.strftime("%d%m%Y.csv")
    file_path = os.path.join(folder, filename)
    os.makedirs(folder, exist_ok=True)
    with open(file_path, mode = 'w', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, delimiter = ',', fieldnames = FIELDSNAME)
        dict_writer.writeheader()
        for coin in coin_data:
            dict_writer.writerow(coin)

#Constant, you can change. TOP250 in CoinMarketCap.com
TOP = 250

if __name__ == "__main__":  

    while True:
        try:
            OUTPUTFILE = datetime.fromtimestamp(time.time()).strftime('%c') + '.csv'
            OUTPUTFILE = OUTPUTFILE.replace(' ', '_')

            today = date.today()
            coin_data = get_coinmarketcap_data ()
            print('CoinMarketCap.com: Current prices are loaded!')

            cc_coinlist = get_cryptocompare_coinlist()
            print('CryptoCompare.com: Coinlist is loaded!')

            coin_data = get_cryptocompare_id (coin_data, cc_coinlist)
            print('CryptoCompare.com: Coin ids are loaded!')

            coingecko_data = get_coingecko_data()
            print('CoinGecko.com: All data is loaded!')

            for i in range(len(coin_data)):
                coin_data[i].update({'Date':time.time()})
                if coin_data[i]['Id']=='':
                    continue
                #Add socialstats data of cryptocompare
                socialstats = get_cryptocompare_socialstats(coin_data[i]['Id'])
                coin_data[i].update(socialstats)
                
                #Add stats data of coingecko
                coin_gecko_data = get_coingecko_data_fromlist (coin_data[i]['Symbol'], coingecko_data)   
                coin_data[i].update(coin_gecko_data)
                
                print('CryptoCompare.com: Data about {} is loaded.'.format(coin_data[i]['Symbol']))
                time.sleep (1)

            #Update the total file    
            save_to_file(coin_data)

            #Save as a separate file in daily folder
            save_to_dailyfile(coin_data)
        except:
            print('something went seriously fucking wrong at ' + datetime.fromtimestamp(time.time()).strftime('%c') )

        time.sleep(3600)
