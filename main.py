from requests_cache import CachedSession
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import concurrent.futures
from dataclasses import dataclass, asdict, field
import pandas as pd
import sqlite3
import os

path = 'result'
if not os.path.exists(path):
    os.makedirs(path)


urls = []
session = CachedSession(
    cache_name='cached'
)

user_agent = UserAgent()
head = {'header': user_agent.random}


@dataclass
class SportLife:
    Product_name: str | None
    Brand: str | None
    Discription: str | None


def start_requests():
    url = 'https://sportlifedistribution.com/brand/5-nutrition?product_list_limit=45'
    response = session.get(url, headers=head)
    print(response.status_code)
    return response


def product_link(response):
    soup = BeautifulSoup(response.text, 'html5lib')
    items_list = soup.findAll('li', class_='product-item')
    print(len(items_list))
    for item in items_list:
        link = item.find('a')['href']
        urls.append(link)


def scraper(url):
    response = session.get(url, headers=head)
    soup = BeautifulSoup(response.text, 'html5lib')
    product_name = soup.find('h1', class_='page-title').span.text
    brand = soup.find('a', class_='amshopby-brand-title-link').text.strip()
    info = soup.find('div', class_='description').div.text.strip()

    product = SportLife(
        Product_name=product_name,
        Brand=brand,
        Discription=info
    )
    print(asdict(product))
    return asdict(product)


@dataclass
class SportLifeList:
    sl_list: list[scraper] = field(default_factory=list)

    def dataframe(self):
        return pd.DataFrame((sbs for sbs in self.sl_list))

    def save_to_csv(self, filename):
        self.dataframe().to_csv(f'{filename}.csv', index=False)

    def save_to_excel(self, filename):
        self.dataframe().to_excel(f'{filename}.xlsx', index=False)

    def save_to_sqlite3(self, filename):
        conn = sqlite3.connect(f'{filename}.db')
        self.dataframe().to_sql(name='scraped_data', con=conn, index=False, if_exists='replace')
        conn.close()

    def save_to_json(self, filename):
        self.dataframe().to_json(f'{filename}.json', orient='records', indent=4, force_ascii=False)


if __name__ == '__main__':
    responses = start_requests()
    product_link(responses)
    with concurrent.futures.ThreadPoolExecutor() as exe:
        prod = exe.map(scraper, urls)
        items_list = SportLifeList()
        for product in prod:
            items_list.sl_list.append(product)
        items_list.save_to_csv('result/prod')
        items_list.save_to_json('result/prod')
        items_list.save_to_excel('result/prod')
        items_list.save_to_sqlite3('result/prod')

