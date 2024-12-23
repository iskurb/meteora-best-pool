import aiohttp
import asyncio
from time import time


headers = {}

MAX_CONCURRENT_REQUESTS = 100
limit_total_tvl = 5000


async def make_request(session, url):
    async with session.get(url, headers=headers) as response:
        text_response = await response.text()
        try:
            response_json = await response.json()
        except Exception as e:
            print(f"Failed to decode JSON: {e} - Response text: {text_response}")
            return None
    return response_json



async def main():
    start_time = time()
    Pools = []
    Pairs = []
    tasks = []

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        print('Starting...')
        #print('total pages -', total_pages)
        for page in range(100):
            url = ('https://app.meteora.ag/clmm-api/pair/all_by_groups?'
                   f'page={page}&'
                   'limit=100&'
                   'unknown=true&'
                   'search_term=sol&'
                   'sort_key=tvl&'
                   'order_by=desc')

            task = asyncio.ensure_future(fetch_with_semaphore(semaphore, session, url))
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        for result in results:
            for pairs in result['groups']:

                #for pool in pairs['pairs']: liquidity += float(pool['liquidity'])
                if 'SOL' in pairs['name'].split('-')[0] and 'SOL' in pairs['name'].split('-')[1]: continue
                if 'SOL' == pairs['name'].split('-')[0].strip() or 'SOL' == pairs['name'].split('-')[1].strip():
                    Pairs.append(pairs)
                    for pool in pairs['pairs']:
                        if float(pool['liquidity']) < limit_total_tvl: continue
                        Pools.append({'address': pool['address'],
                                      'base_fee': pool['base_fee_percentage'],
                                      'liquidity': pool['liquidity']})




    print()
    #print(f"Total pairs: {len(Pairs)}")
    print(f"Total pools: {len(Pools)}")


    await asyncio.sleep(1.5)

    end_time = time()
    elapsed_time = end_time - start_time
    minutes, seconds = divmod(elapsed_time, 60)
    #print(f"Time: {int(minutes)}:{seconds:.0f}")
    #print(Pools)
    return Pools


async def fetch_with_semaphore(semaphore, session, data):
    async with semaphore:
        return await make_request(session, data)


if __name__ == '__main__':
    print()
    asyncio.run(main())

