"""
2022-11-03

csvapi cli, no cache, pool_size=1, analysis=false
-------------->Time execution : 17.989488124847412<--------------
-------------->Time execution : 24.22131085395813<--------------
-------------->Time execution : 21.933547019958496<--------------

csvapi cli, no cache, pool_size=1, analysis=yes
-------------->Time execution : 28.727387189865112<--------------
-------------->Time execution : 27.748358964920044<--------------
-------------->Time execution : 22.15376091003418<--------------

csvapi cli, no cache, pool_size=0, analysis=yes
-------------->Time execution : 27.46714496612549<--------------
-------------->Time execution : 28.398924112319946<--------------
-------------->Time execution : 25.25711679458618<--------------

hypercorn -w3, no cache, pool_size=1, analysis=yes
-------------->Time execution : 17.33577609062195<--------------
-------------->Time execution : 27.747673988342285<--------------
-------------->Time execution : 19.758486032485962<--------------

hypercorn -w3, no cache, pool_size=0, analysis=yes
-------------->Time execution : 23.761262893676758<--------------
-------------->Time execution : 18.91990613937378<--------------
-------------->Time execution : 31.557281017303467<--------------
-------------->Time execution : 31.700807809829712<--------------
-------------->Time execution : 32.8078031539917<--------------
"""

import aiohttp
import asyncio
import time

ANALYSIS = True

URLS_APIFY = [
    'http://localhost:8001/apify?url=http://datanova.legroupe.laposte.fr/explore/dataset/laposte_poincont2/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true',
    'http://localhost:8001/apify?url=https://datanova.legroupe.laposte.fr/explore/dataset/laposte_hexasmal/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true',
    'http://localhost:8001/apify?url=https://people.sc.fsu.edu/~jburkardt/data/csv/snakes_count_10.csv',
    'http://localhost:8001/apify?url=https://people.sc.fsu.edu/~jburkardt/data/csv/snakes_count_100.csv',
    'http://localhost:8001/apify?url=https://people.sc.fsu.edu/~jburkardt/data/csv/snakes_count_1000.csv',
    'http://localhost:8001/apify?url=https://people.sc.fsu.edu/~jburkardt/data/csv/snakes_count_10000.csv'
]


async def fetch_apify(session, url):
    if ANALYSIS:
        url = url.replace('apify?', 'apify?analysis=yes&')
    async with session.get(url) as response:
        res = await response.json()
        return res['endpoint']


async def fetch_api(session, url):
    async with session.get(url) as response:
        return await response.text()


async def main():
    start = time.time()
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(force_close=True)) as session:
        apify_requests = [fetch_apify(session, url) for url in URLS_APIFY]
        endpoints = await asyncio.gather(*apify_requests)
        api_requests = list()
        for endpoint in endpoints:
            for _ in range(20):
                api_requests.append(asyncio.ensure_future(fetch_api(session, endpoint)))
        await asyncio.gather(*api_requests)
    end = time.time()
    print(f"-------------->Time execution : {end - start}<--------------")


if __name__ == '__main__':
    test = asyncio.run(main())
