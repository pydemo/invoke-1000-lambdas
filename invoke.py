import os, sys, time, json
import asyncio
from itertools import chain
from typing import List
import logging
from functools import partial
from pprint import pprint 
# Third Party
import asyncpool
import aiobotocore.session
import aiobotocore.config
_NUM_WORKERS=500
async def execute_lambda( lambda_name: str, key: str, client):
    # Get json content from s3 object
    if 1:
        name=lambda_name
        response = await client.invoke(
            InvocationType='RequestResponse',
            FunctionName=name,
            LogType='Tail',
            Payload=json.dumps({
                'exec_id':key,
                })
            )
    out=[]
    async for event in response['Payload']:
        out.append(event.decode())
    #await asyncio.sleep(1)
    return out
async def submit(lambda_name: str) -> List[dict]:
    """
    Returns list of AWS Lambda outputs executed in parallel
    :param name: name of lambda function
    :return: list of lambda returns
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    session = aiobotocore.session.AioSession()
    config = aiobotocore.config.AioConfig(max_pool_connections=_NUM_WORKERS)
    contents = []
    #client = boto3.client('lambda', region_name='us-west-2')
    async with session.create_client('lambda', region_name='us-west-2', config=config) as client:
        worker_co = partial(execute_lambda, lambda_name)
        async with asyncpool.AsyncPool(None, _NUM_WORKERS, 'lambda_work_queue', logger, worker_co,
                                       return_futures=True, raise_on_join=True, log_every_n=10) as work_pool:
            for x in range(_NUM_WORKERS):
                contents.append(await work_pool.push(x, client))
    # retrieve results from futures
    contents = [c.result() for c in contents]
    return list(chain.from_iterable(contents))
def main(name, files):
    if sys.platform == 'win32':
        _loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(_loop)
        _result = _loop.run_until_complete(submit(name))
    else:
        _loop = asyncio.get_event_loop()
        _result = _loop.run_until_complete(submit(name))
    process = psutil.Process(os.getpid())
    print(f"{__file__}: memory[{process.memory_info().rss/1024:7,.2f}], elapsed {elapsed:0.2f} sec")
