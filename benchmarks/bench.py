import aiohttp
import asyncio
import time


URLS_APIFY = [
    'http://localhost:8001/apify?url=http://datanova.legroupe.laposte.fr/explore/dataset/laposte_poincont2/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true',
    'http://localhost:8001/apify?url=https://datanova.legroupe.laposte.fr/explore/dataset/laposte_hexasmal/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true',
    'http://localhost:8001/apify?url=https://people.sc.fsu.edu/~jburkardt/data/csv/snakes_count_10.csv',
    'http://localhost:8001/apify?url=https://people.sc.fsu.edu/~jburkardt/data/csv/snakes_count_100.csv',
    'http://localhost:8001/apify?url=https://people.sc.fsu.edu/~jburkardt/data/csv/snakes_count_1000.csv',
    'http://localhost:8001/apify?url=https://people.sc.fsu.edu/~jburkardt/data/csv/snakes_count_10000.csv'
]


async def fetch_apify(session, url):
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
            for x in range(20):
                api_requests.append(asyncio.ensure_future(fetch_api(session, endpoint)))
        await asyncio.gather(*api_requests)
    end = time.time()
    print(f"-------------->Time execution : {end - start}<--------------")


if __name__ == '__main__':
    test = asyncio.run(main())
