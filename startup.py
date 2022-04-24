import os
import pathlib
import string
import random
import rel
import json
import websocket
import requests as r

from config import Config
from threading import Thread
from loguru import logger
from timeit import default_timer as timer


# TODO: Is 'block_latest' even needed?
# TODO: Erzeuge events für diverse Ereignisse
# TODO: Zeitmessungen für diverse calls einbauen für debug
# TODO: Global state zusammenbauen
# TODO: Endpunkte ähnlich wie bei BlockService
# TODO: Import von Blocks erlauben
class BlockGrabber:
    cfg = None
    wst = None

    def __init__(self, config: Config):
        self.cfg = config

        while True:
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

    def decode_event(self, message: str) -> (str, str):
        event = json.loads(message)
        return event['event'], event['data']

    def on_message(self, ws, message):
        logger.debug(message)

        event, block = self.decode_event(message)
        block_latest = block['number']

        self.cfg.set('block_latest', block_latest)

        if event == 'latest_block':
            if self.cfg.get('block_current') != block_latest:
                Thread(self.sync_blocks())

        elif event == 'new_block':
            Thread(self.sync_blocks())

    def on_error(self, ws, error):
        logger.debug(error)

    def on_close(self, ws, close_status_code, close_msg):
        logger.debug("Websocket connection closed")
        # TODO: Reconnect

    def on_open(self, ws):
        logger.debug("Opened websocket connection")

    def save_block_in_db(self, block_num: int, content: dict):
        # TODO
        pass

    def save_block_in_file(self, block_num: int, content: dict):
        folder = self.cfg.get('save_blocks_to')

        if not folder: return

        file = os.path.join(folder, f'{block_num}.json')
        os.makedirs(os.path.dirname(file), exist_ok=True)
        with open(file, 'w', encoding='utf-8') as file_obj:
            json.dump(content, file_obj, ensure_ascii=False, sort_keys=True, indent=4)
            logger.debug(f'Saved block {block_num} to file')

    # TODO: Create periodic job that processes blocks on list 'blocks_to_sync'
    # TODO: Execute periodic job only if 'sync_blocks' doesn't run
    # TODO: Periodic job can only run if it isn't already running
    def sync_blocks(self, start: int = None, end: int = None):
        start_time = timer()
        uid = self._id()

        start = start if start else self.cfg.get('block_current')
        end = end if end else self.cfg.get('block_latest')

        self.cfg.set('block_current', self.cfg.get('block_latest'))

        to_sync = list(range(start + 1, end + 1))
        syncing = self.cfg.get('blocks_to_sync')
        do_sync = [x for x in to_sync if x not in syncing]

        logger.debug(f'{uid} TO_SYNC: {to_sync}')
        logger.debug(f'{uid} SYNCING: {syncing}')
        logger.debug(f'{uid} DO_SYNC: {do_sync}')

        syncing.extend(do_sync)
        syncing.sort(key=int)
        self.cfg.set('blocks_to_sync', syncing)

        logger.debug(f'{uid} SYNCING: {syncing}')

        done_sync = list()
        for block_num in do_sync:
            block = self.get_block(block_num)

            self.save_block_in_db(block_num, block)
            self.save_block_in_file(block_num, block)

            done_sync.append(block_num)

        syncing = self.cfg.get('blocks_to_sync')
        todo = [x for x in syncing if x not in done_sync]
        self.cfg.set('blocks_to_sync', todo)

        logger.debug(f'{uid} SYNCING: {todo}')
        logger.debug(f'{uid} Sync time: {timer() - start_time}')

    """
    def get_blocks(self, start: int = None, end: int = None):
        start = start if start else self.cfg.get('block_current')
        end = end if end else self.cfg.get('block_latest')

        while start <= end:
            self.get_block(start)
            start += 1
    """

    def get_block(self, block_num: int):
        source = self.cfg.get('url_block_service')
        if source:
            source += f'/blocks/{block_num}'
        else:
            source = self.cfg.get('url_masternode')
            if source:
                source += f'/blocks?num={block_num}'
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
            logger.error(f'get_block({block_num}) --> {e}')
            return None

    def check_block_files(self):
        folder = '/Users/endogen/Projekte/lamden-blocks'

        for i in range(1, self.cfg.get('block_latest') + 1):
            if not pathlib.Path(os.path.join(folder, f'{i}.json')).is_file():
                print("MISSING BLOCK", i)
                self.get_block(i)

    def panic(self, message):
        logger.error(f'PANIC: {message}')
        # TODO: Notify

    def _id(self, length: int = 8):
        alphabet = string.ascii_uppercase + string.digits
        return ''.join(random.choices(alphabet, k=length))


if __name__ == "__main__":
    cfg = Config('config.json')

    logger.add(
        os.path.join('log', '{time}.log'),
        format='{time} {name} {message}',
        level=cfg.get('log_level'),
        rotation='1 MB')

    BlockGrabber(cfg)
