import gc
import os
import time
import rel
import json
import websocket
import requests as r

from pathlib import Path
from config import Config
from threading import Thread
from loguru import logger
from datetime import datetime, timedelta
from timeit import default_timer as timer
from apscheduler.schedulers.background import BackgroundScheduler


# TODO: Store blocks_missing, blocks_invalid in DB to not accidentally overwrite unrelated data in config
# TODO: Look at every get / set for cfg and decide if load() / dump() is needed
# TODO: Job 'consistancy_check' to check for entire block space excluding & including 'non_existing_blocks'
# TODO: Offer to generate global state
# TODO: Use similar API to BlockService
# TODO: Allow importing blocks via file, GitHub
class BlockGrabber:
    cfg = None
    wst = None
    sch = None

    def __init__(self, config: Config):
        self.cfg = config

        self.__init_jobs()
        self.__init_websocket()

    def __init_jobs(self):
        self.sch = BackgroundScheduler(timezone="Europe/Berlin")

        # TODO: Make sure that jobs are not overlapping

        self.sch.add_job(
            self.sync_blocks,
            name="sync_blocks",
            trigger='interval',
            seconds=self.cfg.get('job_interval_sync'),
            next_run_time=datetime.now() + timedelta(seconds=5),
            max_instances=1)

        # TODO: Add second job for consistency check

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
            Thread(target=self.process_block, args=block)

    def on_error(self, ws, error):
        logger.debug(error)

    def on_close(self, ws, close_status_code, close_msg):
        logger.debug("Websocket connection closed")

    def on_open(self, ws):
        logger.debug("Opened websocket connection")

    def process_block(self, content: dict):
        start_time = timer()

        self.cfg.set('block_latest', content['number'])

        self.save_block_in_db(content)

        if self.cfg.get('save_to_dir'):
            self.save_block_in_file(content)

        logger.debug(f'Processed block {content["number"]} in {timer() - start_time} seconds')

    def save_block_in_db(self, content: dict):
        # TODO: Create new DB connection each time to be thread safe
        pass

    def save_block_in_file(self, content: dict):
        block_dir = self.cfg.get('save_to_dir')
        block_num = content['number']

        file = os.path.join(block_dir, f'{block_num}.json')
        os.makedirs(os.path.dirname(file), exist_ok=True)

        with open(file, 'w', encoding='utf-8') as f:
            json.dump(content, f, sort_keys=True, indent=4)
            logger.debug(f'Saved block {block_num} to file')

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
                    logger.warning(f'No file for block {block_num} in {block_dir}')

                    block = self.get_block(block_num)

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

        logger.warning(f'Missing: {missing}')
        logger.warning(f'Invalid: {invalid}')
        logger.debug(f'Sync job --> Ended after {timer() - start_time} seconds')

    def get_block(self, block_num: int):
        source = self.cfg.get('url_masternode')
        if source:
            source += f'/blocks?num={block_num}'
        else:
            source = self.cfg.get('url_blockservice')
            if source:
                source += f'/blocks/{block_num}'
            else:
                logger.error(f'get_block({block_num}) --> No data source set')
                return None

        try:
            with r.get(source) as data:
                logger.debug(f'Block {block_num} --> {data.text}')

                block = data.json()

                if 'hash' in block:
                    if block['hash'] == 'block-does-not-exist':
                        return None

                return block

        except Exception as e:
            logger.exception(f'get_block({block_num}) --> {e}')
            return None


if __name__ == "__main__":
    cfg = Config('config.json')

    logger.add(
        os.path.join('log', '{time}.log'),
        format='{time} {name} {message}',
        level=cfg.get('log_level'),
        rotation='1 MB',
        diagnose=True)

    BlockGrabber(cfg)
