import gc
import os
import sql
import time
import sys
import json
import websocket
import utils

from block import Block
from tgbot import TelegramBot
from sync import Sync
from database import DB
from config import Config
from threading import Thread
from loguru import logger
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler


# TODO: Switch to Poetry
# TODO: Add possibility to integrate DB dump to start syncing fast
# TODO: Add async block sync pool - distribute block numbers to sync jobs

class LamdenSync:

    db = None
    cfg = None
    wst = None
    bot = None
    sync = None
    scheduler = None

    def __init__(self, config: Config, database: DB, snyc: Sync, tgbot: TelegramBot):
        self.cfg = config
        self.db = database
        self.sync = snyc
        self.bot = tgbot

        self.__init_db()
        self.__init_sync()
        self.__init_websocket()

    def __init_db(self):
        try:
            self.db.execute(sql.create_blocks())
            self.db.execute(sql.create_missing_blocks())
            self.db.execute(sql.create_transactions())
            self.db.execute(sql.create_state_change())
            self.db.execute(sql.create_current_state())
            self.db.execute(sql.create_contracts())
            self.db.execute(sql.create_addresses())
            self.db.execute(sql.create_rewards())
        except Exception as e:
            logger.exception(e)
            raise SystemExit

    def __init_sync(self):
        # TODO: Switch to UTC
        self.scheduler = BackgroundScheduler(timezone="Europe/Berlin")

        self.scheduler.add_job(
            self.sync.sync,
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
                    on_open=lambda ws: self.on_open(ws),
                    on_ping=lambda ws, msg: self.on_ping(ws, msg),
                    on_pong=lambda ws, msg: self.on_pong(ws, msg))

                self.wst = ws.run_forever(
                    ping_interval=self.cfg.get('ws_ping_interval'),
                    ping_timeout=self.cfg.get('ws_ping_timeout'))

            except Exception as e:
                msg = f'Websocket error: {e}'
                logger.exception(msg)
                self.bot.send(msg)
                gc.collect()

            wait_secs = self.cfg.get('ws_reconnect')
            logger.info(f'Websocket reconnect after {wait_secs} seconds')
            time.sleep(wait_secs)

    def on_message(self, ws, msg):
        logger.info(f'New event --> {msg}')

        raw = json.loads(msg)
        event, data = raw['event'], raw['data']

        block = Block(data)

        if event == 'latest_block':
            self.cfg.set('block_latest', block.block_num)
        elif event == 'new_block':
            self.cfg.set('block_latest', block.block_num)
            Thread(target=self.sync.process, args=[block]).start()

    def on_ping(self, ws, msg):
        logger.debug(f'Websocket got a PING')

    def on_pong(self, ws, msg):
        logger.debug(f'Websocket got a PONG')

    def on_error(self, ws, error):
        logger.error(f'Websocket error: {error}')

    def on_close(self, ws, status_code, msg):
        logger.info(f'Websocket connection closed with code {status_code} and message {msg}')

    def on_open(self, ws):
        logger.info("Websocket connection opened")


if __name__ == "__main__":
    utils.create_kill_script('stop')

    db = DB(Config('cfg', 'db.json'))
    cfg = Config('cfg', 'sync.json')

    logger.remove()

    logger.add(
        sys.stderr,
        level=cfg.get('log_level'))

    logger.add(
        os.path.join('log', 'syn_{time}.log'),
        retention=timedelta(days=cfg.get('log_retention')),
        format='{time} {level} {name} {message}',
        level=cfg.get('log_level'),
        rotation='10 MB')

    LamdenSync(
        cfg,
        db,
        Sync(cfg, db),
        TelegramBot(Config('cfg', 'tgbot.json')))
