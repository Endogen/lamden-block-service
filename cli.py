import os
import sys
import typer

from typing import List
from loguru import logger

from block import State
from sync import Sync
from config import Config
from database import DB
from datetime import timedelta
from tgbot import TelegramBot
from timeit import default_timer as timer

# TODO: Check if all blocks are in DB
# TODO: Check if all blocks on HDD
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

sync = Sync(Config(os.path.join('cfg', 'sync.json')), db, tgb)


@app.command()
def sync_blocks(block_nums: List[int]):
    start_time = timer()

    try:
        for block_num in block_nums:
            block = sync.get_block(block_num)

            if block.exists == State.NEW:
                sync.process_block(block)
    except Exception as e:
        logger.exception(e)
        return

    logger.debug(f'Synced in {timer() - start_time} seconds')


@app.command()
def sync_block_range(from_block_num: int, to_block_num: int, check_db: bool = True):
    sync.sync(start=from_block_num, end=to_block_num, check_db=check_db)


@app.command()
def sync_blocks_from(from_block_num: int, check_db: bool = True):
    sync.sync(start=from_block_num, end=0, check_db=check_db)


app()
