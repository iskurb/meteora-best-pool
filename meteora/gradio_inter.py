import asyncio
import aiohttp
from itertools import islice
import meteora
import gradio as gr
from aiohttp import BasicAuth


PROXY_URL = "http://194.113.119.185:6859"
PROXY_AUTH = BasicAuth('untdlcnu', 'qhetm02aqqxp')


def chunked(iterable, size):
    iterator = iter(iterable)
    while chunk := list(islice(iterator, size)):
        yield chunk


async def fetch(session, url, proxy=None, proxy_auth=None, retries=5, delay=30):
    for attempt in range(retries):
        try:
            async with session.get(url, headers={'Accept': 'application/json; version=20230302'},
                                   proxy=proxy, proxy_auth=proxy_auth) as response:
                if response.status == 429:
                    print(f"Received 429, retrying... ({attempt + 1}/{retries})")
                    await asyncio.sleep(delay)
                    continue
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            print(f"Error fetching data: {e}")
            return None


async def limited_fetch(sem, session, url, proxy=None, proxy_auth=None):
    async with sem:
        return await fetch(session, url, proxy, proxy_auth)


async def main(pools, volume_5m, volume_h1):
    base_url = "https://api.geckoterminal.com/api/v2/networks/solana/pools/multi/"

    proxy_pools = pools[:len(pools) // 2]
    no_proxy_pools = pools[len(pools) // 2:]

    proxy_chunks = list(chunked(proxy_pools, 30))
    no_proxy_chunks = list(chunked(no_proxy_pools, 30))

    semaphore_proxy = asyncio.Semaphore(2)
    semaphore_no_proxy = asyncio.Semaphore(2)

    async with aiohttp.ClientSession() as proxy_session, \
               aiohttp.ClientSession() as no_proxy_session:

        tasks_with_proxy = []
        tasks_without_proxy = []

        for chunk in proxy_chunks:
            addresses = ",".join(entry["address"] for entry in chunk)
            url = f"{base_url}{addresses}"
            tasks_with_proxy.append(
                limited_fetch(semaphore_proxy, proxy_session, url, proxy=PROXY_URL, proxy_auth=PROXY_AUTH)
            )

        for chunk in no_proxy_chunks:
            addresses = ",".join(entry["address"] for entry in chunk)
            url = f"{base_url}{addresses}"
            tasks_without_proxy.append(
                limited_fetch(semaphore_no_proxy, no_proxy_session, url)
            )

        use_proxy_list = await asyncio.gather(*tasks_with_proxy)
        not_proxy_list = await asyncio.gather(*tasks_without_proxy)

        for pool in pools:
            for slic in use_proxy_list + not_proxy_list:
                if not slic or 'data' not in slic:
                    continue
                for item_volume in slic['data']:
                    if pool['address'] == item_volume['id'].replace('solana_', '').strip():
                        pool['volume_m5'] = item_volume['attributes']['volume_usd']['m5']
                        pool['volume_h1'] = item_volume['attributes']['volume_usd']['h1']

        final_pools = []
        for pool in pools:
            if pool.get("volume_m5") is not None:
                if float(pool["volume_m5"]) < int(volume_5m) and float(pool["volume_h1"]) < int(volume_h1):
                    continue
                try:
                    rating = ((float(pool["volume_m5"]) * float(pool["base_fee"])) / float(pool["liquidity"])) * 100
                    final_pools.append({pool["address"]: rating, 'name': pool['name']})
                except:
                    continue

        sorted_data = sorted(final_pools, key=lambda x: list(x.values())[0], reverse=True)

        result = []
        for item in sorted_data:
            key, value = next(iter(item.items()))
            if key == 'name':
                continue
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


if __name__ == "__main__":
    demo.launch(share=True)
