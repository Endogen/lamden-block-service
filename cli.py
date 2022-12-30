import os
import sys
import typer

from typing import List
from loguru import logger
from sync import Sync
from config import Config
from database import DB
from datetime import timedelta
from tgbot import TelegramBot

# TODO: Check if all blocks are in DB
# TODO: Check if all blocks on HDD
# TODO: Syncing blocks will mess with sync_start and sync_end - what to do?
# TODO: Add possibility to execute raw SQL statement with no predefined statement

tgb = TelegramBot(Config('cfg', 'tgbot.json'))
db = DB(Config('cfg', 'db.json'))
cfg = Config('cfg', 'cli.json')
app = typer.Typer()

logger.remove()

logger.add(
    sys.stderr,
    level=cfg.get('log_level'))

logger.add(
    os.path.join('log', 'cli_{time}.log'),
    retention=timedelta(days=cfg.get('log_retention')),
    format='{time} {level} {name} {message}',
    level=cfg.get('log_level'),
    rotation='10 MB',
    diagnose=True)

sync = Sync(Config(os.path.join('cfg', 'sync.json')), db)


@app.command()
def sync_blocks(block_nums: List[int]):
    for block_num in block_nums:
        state, block = sync.get_block(block_num)

        if state == State.OK:
            sync.process_block(block)


@app.command()
def sync_block_range(from_block_num: int, to_block_num: int):
    for block_num in range(from_block_num, to_block_num + 1):
        state, block = sync.get_block(block_num)

        if state == State.OK:
            sync.process_block(block)


@app.command()
def sync_blocks_from(start_block_num: int):
    for block_num in range(start_block_num, sync.cfg.get('block_latest') + 1):
        state, block = sync.get_block(block_num)

        if state == State.OK:
            sync.process_block(block)


app()
