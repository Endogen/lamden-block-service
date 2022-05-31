import gc
import os
import time
import rel
import sys
import json
import websocket
import utils

from tgbot import TelegramBot
from sync import Sync
from database import DB
from config import Config
from threading import Thread
from loguru import logger
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler


class LamdenSync:

    db = None
    cfg = None
    wst = None
    bot = None
    block = None
    scheduler = None

    def __init__(self, config: Config, database: DB, block: Sync, tgbot: TelegramBot):
        self.cfg = config
        self.db = database
        self.block = block
        self.bot = tgbot

        self.__init_db()
        self.__init_sync()
        self.__init_websocket()

    def __init_db(self):
        try:
            result = self.db.execute('db_exists', {'name': 'lamden_blocks'})

            if result and result[0][0] != 1:
                # TODO: Does DB creation work automatically?
                self.db.execute('db_create', {'name': 'lamden_blocks'})

            self.db.execute('blocks_create')
            self.db.execute('blocks_invalid_create')
            self.db.execute('blocks_missing_create')
            self.db.execute('transactions_create')
            self.db.execute('state_change_create')
            self.db.execute('current_state_create')
            self.db.execute('contracts_create')
            self.db.execute('addresses_create')
        except Exception as e:
            logger.exception(e)
            raise SystemExit

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
                logger.info(f'Initiating websocket connection...')
                websocket.setdefaulttimeout(self.cfg.get('ws_timeout'))
                ws = websocket.WebSocketApp(self.cfg.get('ws_masternode'),
                    on_message=lambda ws, msg: self.on_message(ws, msg),
                    on_error=lambda ws, msg: self.on_error(ws, msg),
                    on_close=lambda ws, code, msg: self.on_close(ws, code, msg),
                    on_open=lambda ws: self.on_open(ws))

                self.wst = ws.run_forever(dispatcher=rel)

                logger.info('Dispatching...')
                rel.signal(2, rel.abort)
                rel.dispatch()
            except Exception as e:
                msg = f'Websocket connection error: {e}'
                logger.exception(msg)
                self.bot.send(msg)
                gc.collect()

            wait_secs = self.cfg.get('ws_reconnect')
            logger.info(f'Reconnecting after {wait_secs} seconds')
            time.sleep(wait_secs)

    def decode_event(self, message: str) -> (str, str):
        event = json.loads(message)
        return event['event'], event['data']

    def on_message(self, ws, message):
        logger.info(f'New event --> {message}')
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
    utils.create_kill_script('stop')

    cfg = Config(os.path.join('cfg', 'sync.json'))
    db = DB(cfg)

    logger.remove()

    logger.add(
        sys.stderr,
        level=cfg.get('log_level'))

    logger.add(
        os.path.join('log', 'syn_{time}.log'),
        retention=timedelta(days=cfg.get('log_retention')),
        format='{time} {level} {name} {message}',
        level=cfg.get('log_level'),
        rotation='10 MB',
        diagnose=False)

    LamdenSync(
        cfg,
        db,
        Sync(cfg, db),
        TelegramBot(Config(os.path.join('cfg', 'tgbot.json')))
    )
