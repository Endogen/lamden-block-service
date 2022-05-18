import os
import sys
import typer

from typing import List
from loguru import logger
from blocks import Blocks
from config import Config
from database import DB
from datetime import timedelta

# TODO: Sync single block
# TODO: Sync range of blocks
# TODO: Download all blocks from ...
# TODO: Check if all blocks are in DB
# TODO: Check if all blocks on HDD
# TODO: Does DB creation work automatically?

app = typer.Typer()

cfg = Config(os.path.join('cfg', 'config.json'))
db = DB(cfg)

logger.remove()

logger.add(
    sys.stderr,
    level=cfg.get('log_level'))

logger.add(
    os.path.join('log', 'cli_{time}.log'),
    retention=timedelta(days=cfg.get('log_retention')),
    format='{time} {name} {message}',
    level=cfg.get('log_level'),
    rotation='10 MB',
    diagnose=True)

blocks = Blocks(cfg, db)


@app.command()
def sync_block(block_nums: List[int]):
    for block_num in block_nums:
        state, block = blocks.get_block(block_num)
        blocks.process(block)


@app.command()
def sync_block_range(from_block_num: int, to_block_num: int):
    print(f'Syncing blocks from {from_block_num} to {to_block_num}')


app()
