import os
import sql
import json
import time

import requests as r

from pathlib import Path
from config import Config
from database import DB
from loguru import logger
from tgbot import TelegramBot
from timeit import default_timer as timer
from block import Block, Source, WrongBlockDataException, InvalidBlockException


class Sync:

    cfg = None
    db = None
    tgb = None

    def __init__(self, config: Config, database: DB, tgbot: TelegramBot):
        self.cfg = config
        self.db = database
        self.tgb = tgbot

    def process_block(self, block: Block):
        start_time = timer()

        # SAVE BLOCK
        self.insert_block(block)
        logger.debug(f'-> Saved block {block.number} - {timer() - start_time:.4f} seconds')

        # Check for genesis block
        if block.number == 0:

            # SAVE GENESIS STATE
            logger.debug(f'-> Saving genesis block - this will take a while...')
            self.insert_state(block, 'genesis')
            logger.debug(f'-> Saved genesis block - {timer() - start_time:.4f} seconds')
            return

        # SAVE TRANSACTION
        self.insert_tx(block)
        logger.debug(f'-> Saved tx {block.tx_hash} - {timer() - start_time:.4f} seconds')

        # SAVE REWARDS
        self.insert_rewards(block)
        logger.debug(f'-> Saved rewards - {timer() - start_time:.4f} seconds')

        # SAVE REWARDS STATE
        self.insert_state(block, 'rewards')
        logger.debug(f'-> Saved rewards state - {timer() - start_time:.4f} seconds')

        if block.tx_is_valid:

            # SAVE STATE
            self.insert_state(block)
            logger.debug(f'-> Saved state - {timer() - start_time:.4f} seconds')

            # SAVE ADDRESSES
            self.insert_address(block)
            logger.debug(f'-> Saved addresses - {timer() - start_time:.4f} seconds')

            if block.is_new_contract:

                # SAVE CONTRACT
                self.insert_contract(block)
                logger.debug(f'-> Saved contract {block.contract} - {timer() - start_time:.4f} seconds')

        if self.cfg.get('save_blocks_to_file'):

            # SAVE BLOCK TO FILE
            self.save_block_to_file(block)
            logger.debug(f'-> Saved block {block.number} to file - {timer() - start_time:.4f} seconds')

        logger.debug(f'Finished processing block {block.number} - {timer() - start_time:.4f} seconds')

    def insert_block(self, block: Block):
        self.db.execute(sql.insert_block(),
            {'n': block.number, 'h': block.hash, 'b': json.dumps(block.content), 'cr': block.timestamp})

    def insert_tx(self, block: Block):
        self.db.execute(sql.insert_transaction(),
            {'bn': block.number, 'h': block.tx_hash, 't': json.dumps(block.tx), 'cr': block.timestamp})

    def insert_rewards(self, block: Block):
        for rw in block.rewards:
            self.db.execute(sql.insert_reward(),
                {'bn': block.number, 'k': rw['key'], 'v': json.dumps(rw['value']),
                'r': json.dumps(rw['reward']), 'cr': block.timestamp})

            logger.trace(f'-> Reward {rw["key"]} saved')

    def insert_state(self, block: Block, state: str = 'state'):
        if state.lower() == 'state':
            data = block.state
        elif state.lower() == 'rewards':
            data = block.rewards
        elif state.lower() == 'genesis':
            data = block.content['genesis']
        else:
            data = dict()

        for kv in data:
            # Check if state is already known and newer than current data
            data = self.db.execute(sql.select_state(), {'k': kv['key']})

            if data and data[0][0] > block.number:
                logger.trace(f'-> State {kv["key"]} skipped - newer present')
                continue

            self.db.execute(sql.insert_state(),
                {'bn': block.number, 'k': kv['key'], 'v': json.dumps(kv['value']),
                'cr': block.timestamp, 'up': block.timestamp})

            logger.trace(f'-> State {kv["key"]} saved')

    def insert_contract(self, block: Block):
        self.db.execute(sql.insert_contract(),
            {'bn': block.number, 'n': block.contract, 'c': block.code,
            'l1': block.is_lst001, 'l2': block.is_lst002, 'l3': block.is_lst003, 'cr': block.timestamp})

    def insert_address(self, block: Block):
        for address in block.addresses:
            # Check if address is already known and older than current data
            data = self.db.execute(sql.select_address(), {'a': address})

            if data and data[0][0] < block.number:
                logger.trace(f'Address {address} skipped - older present')
                continue

            self.db.execute(sql.insert_address(),
                {'bn': block.number, 'a': address, 'cr': block.timestamp})

            logger.trace(f'-> Address {address} saved')

    def save_block_to_file(self, block: Block):
        block_dir = self.cfg.get('block_dir')
        file = Path(block_dir, f'{block.number}.json')
        os.makedirs(os.path.dirname(file), exist_ok=True)

        with open(file, 'w', encoding='utf-8') as f:
            json.dump(block.content, f, sort_keys=True, indent=4)

    def sync(self, start: int = None, end: int = None, check_db: bool = True):
        start_time = timer()

        logger.debug(f'Sync job --> Started...')

        # Block number to start syncing from
        sync_start = start if start else self.cfg.get('sync_start')
        if not sync_start: sync_start = self.cfg.get('block_latest')

        # Block number to stop syncing at
        sync_end = end if end else self.cfg.get('sync_end')
        if not sync_end: sync_end = 0

        # End sync if both, start and end, are the same
        if sync_start == sync_end:
            logger.debug(f'Sync job --> Synchronized')
            return

        # Should not happen
        if sync_start < sync_end:
            msg = f'Sync job --> sync_start {sync_start} < sync_end {sync_end}'

            # Set sync values to do a full resync
            self.cfg.set('sync_start', None)
            self.cfg.set('sync_end', 0)

            logger.warning(msg)
            self.tgb.send(msg)
            return

        block = self.get_block(sync_start, check_db=check_db)

        while block:
            # Process if data didn't come from DB
            if block.source == Source.WEB:
                self.process_block(block)

            # End sync if current block number is
            # same as sync end or genesis block
            if block.number in (sync_end, 0):
                # New sync end is previous sync start
                self.cfg.set('sync_end', sync_start)
                # New sync start will be block_latest
                self.cfg.set('sync_start', None)
                break

            # Get previous block
            block = self.get_block(block.prev, check_db=check_db)

            # Set sync start to previous block number
            self.cfg.set('sync_start', block.number)

        logger.debug(f'Sync job --> Ended after {timer() - start_time:.4f} seconds')

    def get_block(self, block_id: (int, str), check_db: bool = True) -> Block:
        """ 'block' param can either be block hash or block number """

        # Check if block is already in DB
        if check_db:
            if isinstance(block_id, int):
                # 'block_id' is Block Number
                data = self.db.execute(sql.select_block_by_num(), {'bn': block_id})
            else:
                # 'block_id' is Block Hash
                data = self.db.execute(sql.select_block_by_hash(), {'bh': block_id})

            if data:
                logger.debug(f'Retrieved block {block_id} from database')
                return Block(data[0][2], source=Source.DB)

        # Retrieve from web
        for source in self.cfg.get('retrieve_from'):
            host = source['host']
            wait = source['wait']

            logger.debug(f'Retrieving block {block_id} from {host}')

            if wait:
                logger.debug(f'Waiting for {wait} seconds...')
                time.sleep(wait)

            host = host.replace('{block}', str(block_id))

            try:

                # Get block from web
                with r.get(host) as data:
                    logger.info(f'Block {block_id} --> {data.text}')
                    block = Block(data.json(), source=Source.WEB)

                # Check for genesis block
                if block.number == 0:
                    # Get block from file
                    logger.debug(f'Retrieving genesis block from file')
                    with open(Path('res', 'genesis_block.json')) as f:
                        block = Block(json.load(f), Source.WEB)

                return block

            except InvalidBlockException as e:
                msg = f'Block {block_id} - invalid: {e}'
                logger.exception(msg)
            except WrongBlockDataException as e:
                msg = f'Block {block_id} - wrong data: {e}'
                logger.exception(msg)
            except Exception as e:
                msg = f'Block {block_id} - can not retrieve: {e}'
                logger.exception(msg)

        self.tgb.send(f'Could not retrieve block {block_id}')
