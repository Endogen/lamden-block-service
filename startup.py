import gc
import os
import time
import rel
import json
import websocket

from chain import Chain
from database import DB
from config import Config
from threading import Thread
from loguru import logger
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler


# TODO: Add Telegram integration to notify about events
# TODO: Add API for 'TAU forever lost'
# TODO: Add API for address Toplist (TAU or any other token)
# TODO: Job to remove logs after some time
# TODO: Make sure config can be changed without restarting Block Service
# TODO: Store blocks_missing, blocks_invalid in DB to not accidentally overwrite unrelated data in config
# TODO: Look at every get / set for cfg and decide if load() / dump() is needed
# TODO: Use similar API as default Block Service
# TODO: Allow importing blocks via file and GitHub
# TODO: API for total stamps used for address
# TODO: API for 'network involvement' of an address - how much other addresses transacted with address?
class BlockHustler:

    db = None
    cfg = None
    wst = None
    block = None
    scheduler = None

    def __init__(self, config: Config, database: DB, block: Chain):
        self.cfg = config
        self.db = database
        self.block = block

        self.__init_db()
        self.__init_sync()
        self.__init_websocket()

    def __init_db(self):
        result = self.db.execute('db_exists', {'name': 'lamden_blocks'})

        if result and result[0][0] != 1:
            self.db.execute('db_create', {'name': 'lamden_blocks'})

        self.db.execute('blocks_create')
        self.db.execute('blocks_invalid_create')
        self.db.execute('blocks_missing_create')
        self.db.execute('transactions_create')
        self.db.execute('state_change_create')
        self.db.execute('current_state_create')
        self.db.execute('contracts_create')
        self.db.execute('addresses_create')

    def __init_sync(self):
        self.scheduler = BackgroundScheduler(timezone="Europe/Berlin")

        self.scheduler.add_job(
            self.block.sync,
            name="sync_blocks",
            trigger='interval',
            seconds=self.cfg.get('job_interval_sync'),
            next_run_time=datetime.now() + timedelta(seconds=5),
            max_instances=1)

        self.scheduler.start()

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
            Thread(target=self.block.process, args=[block]).start()

    def on_error(self, ws, error):
        logger.debug(error)

    def on_close(self, ws, close_status_code, close_msg):
        logger.debug("Websocket connection closed")

    def on_open(self, ws):
        logger.debug("Opened websocket connection")


if __name__ == "__main__":
    cfg = Config(os.path.join('cfg', 'config.json'))
    db = DB(cfg)

    logger.add(
        os.path.join('log', '{time}.log'),
        format='{time} {name} {message}',
        level=cfg.get('log_level'),
        rotation='5 MB',
        diagnose=True)

    BlockHustler(cfg, db, Chain(cfg, db))
