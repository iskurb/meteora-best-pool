import asyncio
import aiohttp
from itertools import islice
from time import time
import meteora
from aiohttp import BasicAuth


# HTTP-прокси с авторизацией
PROXY_URL = "http://194.113.119.185:6859"
PROXY_AUTH = BasicAuth('untdlcnu', 'qhetm02aqqxp')
MIN_VOLUME = 30_000


# Функция для разделения списка на группы по 30
def chunked(iterable, size):
    iterator = iter(iterable)
    while chunk := list(islice(iterator, size)):
        yield chunk


# Асинхронная функция для отправки запросов
async def fetch(session, url, proxy=None, proxy_auth=None):
    try:
        async with session.get(url, proxy=proxy, proxy_auth=proxy_auth) as response:
            return await response.json()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None


# Асинхронная функция с ограничением запросов
async def limited_fetch(sem, session, url, proxy=None, proxy_auth=None):
    async with sem:
        return await fetch(session, url, proxy=proxy, proxy_auth=proxy_auth)


# Основная асинхронная функция
async def main(pools):
    base_url = "https://api.geckoterminal.com/api/v2/networks/solana/pools/multi/"

    # Разделяем пулы на две части (с прокси и без)
    proxy_pools = pools[:len(pools) // 2]
    no_proxy_pools = pools[len(pools) // 2:]

    # Разбиваем на чанки по 30
    proxy_chunks = list(chunked(proxy_pools, 30))
    no_proxy_chunks = list(chunked(no_proxy_pools, 30))

    # Семафоры для ограничения
    semaphore_proxy = asyncio.Semaphore(10)
    semaphore_no_proxy = asyncio.Semaphore(10)

    # Создаём клиентские сессии
    connector_no_proxy = aiohttp.TCPConnector()
    async with aiohttp.ClientSession() as proxy_session, \
               aiohttp.ClientSession(connector=connector_no_proxy) as no_proxy_session:

        tasks_with_proxy = []
        tasks_without_proxy = []

        # Запросы с прокси
        for chunk in proxy_chunks:
            addresses = ",".join(entry["address"] for entry in chunk)
            url = f"{base_url}{addresses}"
            tasks_with_proxy.append(
                limited_fetch(semaphore_proxy, proxy_session, url, proxy=PROXY_URL, proxy_auth=PROXY_AUTH)
            )

        # Запросы без прокси
        for chunk in no_proxy_chunks:
            addresses = ",".join(entry["address"] for entry in chunk)
            url = f"{base_url}{addresses}"
            tasks_without_proxy.append(
                limited_fetch(semaphore_no_proxy, no_proxy_session, url)
            )

        # Параллельный запуск
        use_proxy_list = await asyncio.gather(*tasks_with_proxy)
        not_proxy_list = await asyncio.gather(*tasks_without_proxy)

        # Обновляем volume у пулов
        for pool in pools:
            for slic in use_proxy_list:
                if not slic: continue
                for item_volume in slic.get('data', []):
                    if pool['address'] == item_volume['id'].replace('solana_', '').strip():
                        pool['volume'] = item_volume['attributes']['volume_usd']['m5']
            for slic in not_proxy_list:
                if not slic: continue
                for item_volume in slic.get('data', []):
                    if pool['address'] == item_volume['id'].replace('solana_', '').strip():
                        pool['volume'] = item_volume['attributes']['volume_usd']['m5']

        # Фильтрация и рейтинг
        final_pools = []
        for pool in pools:
            if float(pool.get("volume", 0)) < MIN_VOLUME:
                continue
            try:
                rating = ((float(pool["volume"]) * float(pool["base_fee"])) / float(pool["liquidity"])) * 100
                final_pools.append({pool["address"]: rating})
            except Exception as e:
                print(f"Error in rating calc for {pool['address']}: {e}")

        # Сортировка по рейтингу
        sorted_data = sorted(final_pools, key=lambda x: list(x.values())[0], reverse=True)

        # Вывод
        for item in sorted_data:
            key, value = next(iter(item.items()))
            print(f"{key}: {round(value, 2)}")


if __name__ == '__main__':
    start_time = time()
    print("⏳ Получаем пулы из meteora...")
    Pools = asyncio.run(meteora.main())
    print("✅ Получено пулов:", len(Pools))
    asyncio.run(main(Pools))
    print(f"\n⏱ Выполнено за {round(time() - start_time, 2)} сек.")
