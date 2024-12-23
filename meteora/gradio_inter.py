import asyncio
import aiohttp
from itertools import islice
from time import time
import meteora
from aiohttp_socks import ProxyConnector
import gradio as gr

# SOCKS5-прокси-сервер
PROXY_URL = "socks5://iskurb:iskurb@5.35.45.112:1080"
min_volume = 500

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

    proxy_pools = pools[:len(pools) // 2]  # Первая половина пули идет через прокси
    no_proxy_pools = pools[len(pools) // 2:]  # Остальная часть без прокси

    proxy_chunks = list(chunked(proxy_pools, 30))
    no_proxy_chunks = list(chunked(no_proxy_pools, 30))

    semaphore_proxy = asyncio.Semaphore(29)
    semaphore_no_proxy = asyncio.Semaphore(29)

    connector_proxy = ProxyConnector.from_url(PROXY_URL)
    connector_no_proxy = aiohttp.TCPConnector()

    async with aiohttp.ClientSession(connector=connector_proxy) as proxy_session, \
            aiohttp.ClientSession(connector=connector_no_proxy) as no_proxy_session:
        tasks_with_proxy = []
        tasks_without_proxy = []

        for chunk in proxy_chunks:
            addresses = ",".join(entry["address"] for entry in chunk)
            url = f"{base_url}{addresses}"
            tasks_with_proxy.append(limited_fetch(semaphore_proxy, proxy_session, url))

        for chunk in no_proxy_chunks:
            addresses = ",".join(entry["address"] for entry in chunk)
            url = f"{base_url}{addresses}"
            tasks_without_proxy.append(limited_fetch(semaphore_no_proxy, no_proxy_session, url))

        use_proxy_list = await asyncio.gather(*tasks_with_proxy)
        not_proxy_list = await asyncio.gather(*tasks_without_proxy)

        for pool in pools:
            for slic in use_proxy_list:
                for item_volume in slic['data']:
                    if pool['address'] == item_volume['id'].replace('solana_', '').strip():
                        pool['volume'] = item_volume['attributes']['volume_usd']['m5']
            for slic in not_proxy_list:
                for item_volume in slic['data']:
                    if pool['address'] == item_volume['id'].replace('solana_', '').strip():
                        pool['volume'] = item_volume['attributes']['volume_usd']['m5']

        final_pools = []
        for pool in pools:
            if pool["volume"] != None:
                if float(pool["volume"]) < min_volume: continue
                rating = ((float(pool["volume"]) * float(pool["base_fee"])) / float(pool["liquidity"])) * 100
                final_pools.append({pool["address"]: rating})

        sorted_data = sorted(final_pools, key=lambda x: list(x.values())[0], reverse=True)

        result = []
        for item in sorted_data:
            key, value = next(iter(item.items()))
            result.append(f"{key}: {round(value, 2)}")

        return "\n".join(result)

# Обёртка для запуска асинхронного процесса через Gradio
def run_analysis():
    pools = asyncio.run(meteora.main())
    result = asyncio.run(main(pools))
    return result


# Создание интерфейса Gradio
interface = gr.Interface(
    fn=run_analysis,
    inputs=[],
    outputs="text",
    title="Pool Analysis",
    description="Нажмите на кнопку 'Старт' для выполнения анализа."
)

# Запуск приложения Gradio
if __name__ == "__main__":
    interface.launch(share=True)
