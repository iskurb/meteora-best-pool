import asyncio
import aiohttp
from itertools import islice
import meteora
from aiohttp_socks import ProxyConnector
import gradio as gr

# SOCKS5-прокси-сервер
PROXY_URL = "socks5://iskurb:iskurb@5.35.45.112:1080"

# Функция для разделения списка на группы по 30
def chunked(iterable, size):
    iterator = iter(iterable)
    while chunk := list(islice(iterator, size)):
        yield chunk

# Асинхронная функция для отправки запросов
async def fetch(session, url, delay=1):
    try:
        await asyncio.sleep(delay)
        async with session.get(url) as response:
            #print(response.status)
            return await response.json()
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

# Асинхронная функция с ограничением запросов
async def limited_fetch(sem, session, url):
    async with sem:  # Ограничиваем количество запросов в минуту
        return await fetch(session, url)

# Основная асинхронная функция
async def main(pools, volume_5m, volume_h1):
    base_url = "https://api.geckoterminal.com/api/v2/networks/solana/pools/multi/"

    proxy_pools = pools[:len(pools) // 2]  # Первая половина пули идет через прокси
    no_proxy_pools = pools[len(pools) // 2:]  # Остальная часть без прокси

    proxy_chunks = list(chunked(proxy_pools, 30))
    no_proxy_chunks = list(chunked(no_proxy_pools, 30))

    semaphore_proxy = asyncio.Semaphore(10)
    semaphore_no_proxy = asyncio.Semaphore(10)

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
                        pool['volume_m5'] = item_volume['attributes']['volume_usd']['m5']
                        pool['volume_h1'] = item_volume['attributes']['volume_usd']['h1']
            for slic in not_proxy_list:
                for item_volume in slic['data']:
                    if pool['address'] == item_volume['id'].replace('solana_', '').strip():
                        pool['volume_m5'] = item_volume['attributes']['volume_usd']['m5']
                        pool['volume_h1'] = item_volume['attributes']['volume_usd']['h1']

        final_pools = []
        for pool in pools:
            if pool["volume_m5"] != None:
                if float(pool["volume_m5"]) < int(volume_5m) and float(pool["volume_h1"]) < int(volume_h1): continue
                rating = ((float(pool["volume_m5"]) * float(pool["base_fee"])) / float(pool["liquidity"])) * 100
                final_pools.append({pool["address"]: rating, 'name': pool['name']})


        sorted_data = sorted(final_pools, key=lambda x: list(x.values())[0], reverse=True)

        result = []
        # for item in sorted_data:
        #     key, value = next(iter(item.items()))
        #     str(value).rjust('<pre>')
        #     html_sting = f"<a href='https://app.meteora.ag/dlmm/{key}' target='_blank'>{item['name']}</a>  🌟 {round(value, 1)} 🌟"
        #     result.append(html_sting)
        # return "<br>".join(result[0:25])

        for item in sorted_data:
            key, value = next(iter(item.items()))
            # Генерация строки HTML
            html_string = f"""
                <tr>
                    <td style="padding: 5px; text-align: center;">
                        <a href='https://app.meteora.ag/dlmm/{key}' target='_blank'>{item['name']}</a>
                    </td>
                    <td style="padding: 5px; text-align: center;">
                        🌟 {round(value, 1)} 🌟
                    </td>
                </tr>
                """
            result.append(html_string)

        # Оборачиваем строки в таблицу
        html_table = f"""
                <table style="width: 25%; font-family: Arial, sans-serif; font-size: 12px;">
                    {''.join(result[:20])}
                </table>
                """

        return html_table

def run_analysis(volume_5m, volume_h1, min_tvl):
    pools = asyncio.run(meteora.main(int(min_tvl)))
    result = asyncio.run(main(pools, volume_5m, volume_h1))
    return result


with gr.Blocks() as demo:
    gr.Markdown("# Search for the best pool meteora")

    with gr.Row():
        volume_5m = gr.Textbox(value='800', label='Minimum volume in 5 minutes $')
        volume_h1 = gr.Textbox(value='12000', label='Minimum volume per hour $')
        min_tvl = gr.Textbox(value='3000', label='Minimum TVL $')


    output = gr.HTML(label="Результат анализа")

    start_btn = gr.Button('Start')
    start_btn.click(fn=run_analysis, show_progress='full', outputs=output, inputs=[volume_5m, volume_h1, min_tvl])


# Запуск приложения Gradio
if __name__ == "__main__":
    demo.launch(share=True) #share=True
