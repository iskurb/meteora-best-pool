import asyncio

import aiohttp
from itertools import islice
from time import time
import meteora
from aiohttp_socks import ProxyConnector



# SOCKS5-прокси-сервер
PROXY_URL = "socks5://iskurb:iskurb@5.35.45.112:1080"
min_volume = 30_000


# Функция для разделения списка на группы по 30
def chunked(iterable, size):
    iterator = iter(iterable)
    while chunk := list(islice(iterator, size)):
        yield chunk


# Асинхронная функция для отправки запросов
async def fetch(session, url):
    try:
        async with session.get(url) as response:
            return await response.json()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None


# Асинхронная функция с ограничением запросов
async def limited_fetch(sem, session, url):
    async with sem:  # Ограничиваем количество запросов в минуту
        return await fetch(session, url)


# Основная асинхронная функция
async def main(pools):
    base_url = "https://api.geckoterminal.com/api/v2/networks/solana/pools/multi/"

    # Разделяем пулы на две части (с прокси и без прокси)
    proxy_pools = pools[:len(pools) // 2]  # Первая половина пули идет через прокси
    no_proxy_pools = pools[len(pools) // 2:]  # Остальная часть без прокси

    # Создаем группы по 30
    proxy_chunks = list(chunked(proxy_pools, 30))
    no_proxy_chunks = list(chunked(no_proxy_pools, 30))

    # Семафоры для ограничения запросов (29 запросов в минуту)
    semaphore_proxy = asyncio.Semaphore(10)
    semaphore_no_proxy = asyncio.Semaphore(10)

    # Создаем соединения
    connector_proxy = ProxyConnector.from_url(PROXY_URL)
    connector_no_proxy = aiohttp.TCPConnector()

    async with aiohttp.ClientSession(connector=connector_proxy) as proxy_session, \
            aiohttp.ClientSession(connector=connector_no_proxy) as no_proxy_session:
        tasks_with_proxy = []
        tasks_without_proxy = []

        # Обрабатываем запросы с прокси
        for chunk in proxy_chunks:
            addresses = ",".join(entry["address"] for entry in chunk)  # Формируем адреса через запятую
            url = f"{base_url}{addresses}"  # Формируем полный URL
            tasks_with_proxy.append(limited_fetch(semaphore_proxy, proxy_session, url))

        # Обрабатываем запросы без прокси
        for chunk in no_proxy_chunks:
            addresses = ",".join(entry["address"] for entry in chunk)  # Формируем адреса через запятую
            url = f"{base_url}{addresses}"  # Формируем полный URL
            tasks_without_proxy.append(limited_fetch(semaphore_no_proxy, no_proxy_session, url))

        # Выполняем запросы параллельно
        use_proxy_list = await asyncio.gather(*tasks_with_proxy)
        not_proxy_list = await asyncio.gather(*tasks_without_proxy)


        for pool in pools:
            for slic in use_proxy_list:
                for item_volume in slic['data']:
                    if pool['address'] == item_volume['id'].replace('solana_', '').strip():
                        pool['volume'] = item_volume['attributes']['volume_usd']['m5'] #h1
            for slic in not_proxy_list:
                for item_volume in slic['data']:
                    if pool['address'] == item_volume['id'].replace('solana_', '').strip():
                        pool['volume'] = item_volume['attributes']['volume_usd']['m5'] #h1

        #print(pools)

        final_pools = []
        for pool in pools:
            if float(pool["volume"]) < min_volume: continue
            rating = ((float(pool["volume"]) * float(pool["base_fee"])) / float(pool["liquidity"])) * 100
            final_pools.append({pool["address"]: rating})

        sorted_data = sorted(final_pools, key=lambda x: list(x.values())[0], reverse=True)

        for item in sorted_data:
            key, value = next(iter(item.items()))
            print(f"{key}: {round(value, 2)}")



if __name__ == '__main__':
    start_time = time()
    print()
    Pools = asyncio.run(meteora.main())
    print()
    asyncio.run(main(Pools))
