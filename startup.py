import gc
import os
import time
import rel
import json
import websocket
import requests as r
from requests import Response

from database import DB
from pathlib import Path
from config import Config
from threading import Thread
from loguru import logger
from datetime import datetime, timedelta
from timeit import default_timer as timer
from apscheduler.schedulers.background import BackgroundScheduler


# TODO: Job to remove logs after some time
# TODO: Make sure config can be changed without restarting Block Service
# TODO: Store blocks_missing, blocks_invalid in DB to not accidentally overwrite unrelated data in config
# TODO: Look at every get / set for cfg and decide if load() / dump() is needed
# TODO: Use similar API as default Block Service
# TODO: Allow importing blocks via file and GitHub
class BlockGrabber:
    cfg = None
    wst = None
    sch = None
    db = None

    def __init__(self, config: Config, database: DB):
        self.cfg = config
        self.db = database

        self.__init_sync()
        self.__init_websocket()

    def __init_sync(self):
        self.sch = BackgroundScheduler(timezone="Europe/Berlin")

        self.sch.add_job(
            self.sync_blocks,
            name="sync_blocks",
            trigger='interval',
            seconds=self.cfg.get('job_interval_sync'),
            next_run_time=datetime.now() + timedelta(seconds=5),
            max_instances=1)

        self.sch.start()

    def __init_websocket(self):
        while True:
            try:
                ws = websocket.WebSocketApp(self.cfg.get('wss_masternode'),
                    on_message=lambda ws, msg: self.on_message(ws, msg),
                    on_error=lambda ws, msg: self.on_error(ws, msg),
                    on_close=lambda ws, code, msg: self.on_close(ws, code, msg),
                    on_open=lambda ws: self.on_open(ws))

                self.wst = Thread(target=ws.run_forever, kwargs={'dispatcher': rel})
                self.wst.daemon = True
                self.wst.start()

                rel.signal(2, rel.abort)
                rel.dispatch()
            except Exception as e:
                logger.exception(f'Websocket connection error: {e}')
                gc.collect()

            wait_secs = self.cfg.get('reconnect_after')
            logger.debug(f'Reconnecting after {wait_secs} seconds')
            time.sleep(wait_secs)

    def decode_event(self, message: str) -> (str, str):
        event = json.loads(message)
        return event['event'], event['data']

    def on_message(self, ws, message):
        logger.debug(f'New event --> {message}')
        event, block = self.decode_event(message)

        if event == 'latest_block':
            self.cfg.set('block_latest', block['number'])
        elif event == 'new_block':
            self.cfg.set('block_latest', block['number'])
            Thread(target=self.process_block, args=[block]).start()

    def on_error(self, ws, error):
        logger.debug(error)

    def on_close(self, ws, close_status_code, close_msg):
        logger.debug("Websocket connection closed")

    def on_open(self, ws):
        logger.debug("Opened websocket connection")

    def process_block(self, content: dict):
        start_time = timer()

        # TODO: Combine all this in one method? More efficient
        self.save_block_in_db(content)
        self.save_transaction_in_db(content)
        self.save_state_change_in_db(content)
        self.save_current_state_in_db(content)

        if self.cfg.get('save_to_dir'):
            self.save_block_in_file(content)

        logger.debug(f'Processed block {content["number"]} in {timer() - start_time} seconds')

    def save_block_in_file(self, content: dict):
        block_dir = self.cfg.get('save_to_dir')
        block_num = content['number']

        file = os.path.join(block_dir, f'{block_num}.json')
        os.makedirs(os.path.dirname(file), exist_ok=True)

        with open(file, 'w', encoding='utf-8') as f:
            json.dump(content, f, sort_keys=True, indent=4)
            logger.debug(f'Saved block {block_num} in file')

    def save_block_in_db(self, content: dict):
        self.db.execute('insert_block', {'bn': content['number'], 'b': json.dumps(content)})
        logger.debug(f'Saved block {content["number"]} in database')

    def save_transaction_in_db(self, content: dict):
        for subblock in self._get_block_without_state(content)['subblocks']:
            for tx in subblock['transactions']:
                self.db.execute(
                    'insert_transaction',
                    {'h': tx['hash'], 't': json.dumps(tx), 'b': content['number']})

                logger.debug(f'Saved Transaction {tx["hash"]} in database')

    def save_state_change_in_db(self, content: dict):
        for subblock in content['subblocks']:
            for tx in subblock['transactions']:
                if 'state' in tx:
                    self.db.execute(
                        'insert_state_change',
                        {'txh': tx['hash'], 's': json.dumps(tx['state'])})

                    logger.debug(f'Saved State Change from {tx["hash"]} in database')
                else:
                    logger.debug(f'State Change: No state change in tx {tx["hash"]}')

    def save_current_state_in_db(self, content: dict):
        for subblock in content['subblocks']:
            for tx in subblock['transactions']:
                if 'state' in tx:
                    for kv in tx['state']:
                        key = kv['key']
                        value = kv['value']

                        if type(value) is dict:
                            value = next(iter(value.values()))

                        self.db.execute(
                            'insert_current_state',
                            {'txh': tx['hash'], 'k': key, 'v': value, 's': json.dumps(kv)})

                    logger.debug(f'Saved Current State from {tx["hash"]} in database')
                else:
                    logger.debug(f'Current State: No state change in tx {tx["hash"]}')

    def save_contract_in_db(self, content: dict):
        pass

    def save_address_in_db(self, content: dict):
        pass

    # TODO: Check in DB if block exists. If not, check if it is part of 'blocks_invalid'
    def sync_blocks(self, start: int = None, end: int = None):
        start_time = timer()

        start = start if start else self.cfg.get('block_current')
        end = end if end else self.cfg.get('block_latest')

        to_sync = list(range(start + 1, end + 1))
        missing = self.cfg.get('blocks_missing')
        invalid = self.cfg.get('blocks_invalid')

        to_sync.extend(missing)
        to_sync.sort(key=int)

        if not to_sync:
            logger.debug(f'Sync job --> Synchronized!')
            return

        logger.debug(f'Sync job --> Started...')
        logger.debug(f'Missing: {missing}')
        logger.debug(f'To Sync: {to_sync}')
        logger.debug(f'Invalid: {invalid}')

        block_dir = self.cfg.get('save_to_dir')
        sleep_for = self.cfg.get('block_sync_wait')

        missing = list()
        for block_num in to_sync:
            # TODO: Check block data in DB - If not present: self.save_block_in_db()

            if block_dir:
                if Path(os.path.join(block_dir, f'{block_num}.json')).is_file():
                    logger.debug(f'Block {block_num} already exists')
                else:
                    time.sleep(sleep_for)
                    logger.debug(f'No file for block {block_num} in {block_dir}')

                    _, block = self.get_block(block_num)

                    if not block:
                        missing.append(block_num)
                        continue
                    if 'error' in block:
                        if block['error'] == 'Block not found.':
                            invalid.append(block_num)
                        else:
                            missing.append(block_num)
                        continue

                    self.process_block(block)

        # TODO: Once data is stored in DB, set directly after each block
        self.cfg.set('block_current', end)
        self.cfg.set('blocks_missing', missing)
        self.cfg.set('blocks_invalid', invalid)

        logger.debug(f'Missing: {missing}')
        logger.debug(f'Invalid: {invalid}')
        logger.debug(f'Sync job --> Ended after {timer() - start_time} seconds')

    def is_block_valid(self, response: Response) -> (bool, dict):
        block_data = response.json()

        if 'error' in block_data:
            return False, block_data
        if block_data['hash'] == 'block-does-not-exist':
            return False, block_data

        return True, block_data

    # TODO: Add possibility to use more than one Block Service
    def get_block(self, block_num: int) -> (bool, dict):
        source_bs = self.cfg.get('url_blockservice').replace('{block_num}', str(block_num))
        source_mn = self.cfg.get('url_masternode').replace('{block_num}', str(block_num))

        try:
            with r.get(source_bs) as data:
                logger.debug(f'Block {block_num} via BlockService --> {data.text}')
                block_valid, block = self.is_block_valid(data)
                if block_valid: return block_valid, block

            logger.warning('No valid block data from BlockService. Trying Masternode...')

            with r.get(source_mn) as data:
                logger.debug(f'Block {block_num} via Masternode --> {data.text}')
                return self.is_block_valid(data)

        except Exception as e:
            logger.exception(f'get_block({block_num}) --> {e}')
            return False, None

    def _get_block_without_state(self, d: dict):
        new_d = dict(d)
        if 'state' in new_d:
            del new_d['state']
        return new_d


if __name__ == "__main__":
    cfg = Config(os.path.join('cfg', 'config.json'))

    logger.add(
        os.path.join('log', '{time}.log'),
        format='{time} {name} {message}',
        level=cfg.get('log_level'),
        rotation='5 MB',
        diagnose=True)

    BlockGrabber(cfg, DB(cfg))
