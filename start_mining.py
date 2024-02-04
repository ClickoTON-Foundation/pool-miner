from datetime import datetime
from httpx import AsyncClient
from binascii import hexlify
from tonsdk.boc import Cell
import subprocess
import logging
import asyncio
import httpx
import json
import yaml
import os

counter = 0
logging.basicConfig(filename='info.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

async def mine_pow(gpu, command, path, seed):
    try:
        process = await asyncio.create_subprocess_exec(*command.split(),
                                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        await process.communicate()
    except subprocess.CalledProcessError:
        pass
    except asyncio.CancelledError:
        process.terminate()
        await process.wait()
        raise

    boc = None
    try:
        with open(path, 'rb') as f:
            boc = f.read()
        os.remove(path)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(e)

    if not boc:
        return False

    return seed, boc, gpu

async def mine_task(task: dict) -> bool:
    global counter
    
    seed = task['seed']
    complexity = task['complexity']
    iterations = task['iterations']
    giver = task['giver']
    pool = task['pool']
    task_id = task['task_id']
    timeout = task['timeout']

    tasks_ = []

    for gpu in range(config["GPUS"]):
        file = hexlify(os.urandom(8)).decode('utf-8') + '.boc'
        path = 'bocs/' + file
        command = f'{config["MINER"]} -g {gpu} -F {config["BOOST_FACTORS"][gpu]} -t {timeout} {pool} {seed} {complexity} {iterations} {giver} {path}'

        task = asyncio.create_task(mine_pow(gpu, command, path, seed))
        tasks_.append(task)

    done, _ = await asyncio.wait(tasks_, return_when=asyncio.FIRST_COMPLETED)

    for task in done:
        result = task.result()
        if result and isinstance(result, tuple):
            seed, boc, gpu = result

            for remaining_task in tasks_:
                if not remaining_task.done():
                    remaining_task.cancel()

            try:
                Cell.one_from_boc(boc)
            except Exception as e:
                print(e)
                return False

            async with AsyncClient() as client:
                r = (await client.post(config["POOL_API"] + '/solve_task', json={
                    'task_id': task_id,
                    'boc': boc.hex(),
                    'address': config["REWARD_ADDRESS"]
                })).json()

                if not r['ok']:
                    info = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: {r['error']} {seed[:8]}"
                    logging.warning(info)
                    print(info)
                    return True

                counter += 1
                info = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: [{gpu}]     mined {seed[:8]} {counter}\nBalance: {r['data']['balance']}"
                logging.info(info)
                print(info)
            return True

    return False

old_task = {'task_id': None}
async def mine() -> None:
    global old_task, counter

    async with AsyncClient() as client:
        task = (await client.get(config["POOL_API"] + '/request_task')).json()['data']
        try:
            if 'task_id' in task and 'task_id' in old_task:
                if old_task['task_id'] != task['task_id']:

                    info = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: mining new task... {task['seed'][:8]}, {counter}"
                    logging.info(info)
                    print(info)
                    
                    await mine_task(task)
                    old_task = task

                    info = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: waiting for new..."
                    logging.info(info)
                    print(info)

            await asyncio.sleep(0.1)

        except KeyError as e:
            logging.warning('KeyError: ', e)
            print(f'KeyError: {e}')
            old_task = {'task_id': None}

async def main() -> None:
    while True:
        try:
            await mine()
        except httpx.RequestError as e:
            logging.error(f'RequestError: {e}', exc_info=True)
        except json.decoder.JSONDecodeError as e:
            logging.error(f'JSONDecodeError: {e}', exc_info=True)
        except Exception as e:
            logging.error(f'Exception: {e}', exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())