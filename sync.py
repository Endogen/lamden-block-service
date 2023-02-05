import sql
import json
import time
import pytz

import requests as r

from pathlib import Path
from config import Config
from database import DB
from block import Block
from loguru import logger
from tgbot import TelegramBot
from datetime import datetime
from timeit import default_timer as timer
from requests.exceptions import ConnectionError, HTTPError, Timeout, TooManyRedirects, RequestException


# TODO: Do i really see errors if they happen?
class Sync:

    db = None
    cfg = None
    tgb = None

    def __init__(self, config: Config, database: DB, tgbot: TelegramBot):
        self.cfg = config
        self.db = database
        self.tgb = tgbot

    def process_genesis_block(self):
        total_time = timer()

        genesis_block_dir = self.cfg.get('genesis_block_dir')
        genesis_block = Path(genesis_block_dir, 'genesis_block.json')

        # Check if genesis block file exists
        if not genesis_block.is_file():
            logger.error(f'File does not exist: {genesis_block}')
            return

        # Load content of genesis block
        with open(Path(genesis_block)) as f:
            block_data = json.load(f)

        # Set valid timestamp
        block_data['hlc_timestamp'] = '-infinity'

        # Save block
        start_time = timer()
        self.insert_block(Block(block_data))
        logger.debug(f'-> Saved genesis block - {timer() - start_time:.3f} seconds')

        # Check if state-change files exist
        state_changes_list = Path(genesis_block_dir).glob('**/state_changes*.json')
        genesis_state = list()

        # Merge state into one list
        for path in state_changes_list:
            with open(Path(path)) as f:
                logger.debug(f'-> Adding genesis state changes from {path}')
                genesis_state += json.load(f)

        start_time = timer()
        logger.debug(f'-> Saving genesis state...')

        # Save genesis state in database
        for kv in genesis_state:
            self.db.execute(sql.insert_state(),
                {'bn': 0, 'k': kv['key'], 'v': json.dumps(kv['value']), 'cr': '-infinity', 'up': '-infinity'})

        logger.debug(f'-> Saved genesis state - {timer() - start_time:.3f} seconds')

        state = dict()

        logger.debug(f'-> Transforming genesis state...')

        # Create proper dict from state
        for entry in genesis_state:
            state[entry['key']] = entry['value']

        start_time = timer()
        logger.debug(f'-> Saving genesis contracts...')

        # Identify contracts
        for key, value in state.items():
            if key.endswith('.__code__'):
                code = value
                name = key[:key.index('.__code__')]
                submitted = state[name + '.__submitted__']['__time__']
                submitted = datetime(*submitted, tzinfo=pytz.UTC)

                lst001 = Block.con_is_lst001(code)
                lst002 = Block.con_is_lst002(code)
                lst003 = Block.con_is_lst003(code)

                self.db.execute(sql.insert_contract(),
                    {'bn': 0, 'n': name, 'c': code, 'l1': lst001, 'l2': lst002, 'l3': lst003, 'cr': submitted})

        logger.debug(f'-> Saved genesis contracts - {timer() - start_time:.3f} seconds')
        logger.debug(f'Finished processing genesis block - {timer() - total_time:.3f} seconds')

    def process_block(self, block: Block):
        total_time = timer()

        # SAVE BLOCK
        start_time = timer()
        self.insert_block(block)
        logger.debug(f'-> Saved block {block.number} - {timer() - start_time:.3f} seconds')

        # SAVE TRANSACTION
        start_time = timer()
        self.insert_tx(block)
        logger.debug(f'-> Saved tx {block.tx_hash} - {timer() - start_time:.3f} seconds')

        # SAVE REWARDS
        start_time = timer()
        self.insert_rewards(block)
        logger.debug(f'-> Saved rewards - {timer() - start_time:.3f} seconds')

        # SAVE REWARDS STATE
        start_time = timer()
        self.insert_state(block, 'rewards')
        logger.debug(f'-> Saved rewards state - {timer() - start_time:.3f} seconds')

        if block.tx_is_valid:

            # SAVE STATE
            start_time = timer()
            self.insert_state(block)
            logger.debug(f'-> Saved state - {timer() - start_time:.3f} seconds')

            # SAVE ADDRESSES
            start_time = timer()
            self.insert_address(block)
            logger.debug(f'-> Saved addresses - {timer() - start_time:.3f} seconds')

            if block.is_new_contract:

                # SAVE CONTRACT
                start_time = timer()
                self.insert_contract(block)
                logger.debug(f'-> Saved contract {block.contract} - {timer() - start_time:.3f} seconds')

        if self.cfg.get('save_blocks_to_file'):

            # SAVE BLOCK TO FILE
            start_time = timer()
            self.save_block_to_file(block)
            logger.debug(f'-> Saved block {block.number} to file - {timer() - start_time:.3f} seconds')

        logger.debug(f'Finished processing block {block.number} - {timer() - total_time:.3f} seconds')

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
        else:
            data = dict()

        for kv in data:
            # Check if state is already known and newer than current data
            data = self.db.execute(sql.select_raw_state(), {'k': kv['key']})

            if data and data[0] and data[0][0] and data[0][0]['block_num'] > block.number:
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
        Path(block_dir).mkdir(parents=True, exist_ok=True)
        block_file = Path(block_dir, f'{block.number}.json')

        with open(block_file, 'w', encoding='utf-8') as f:
            json.dump(block.content, f, sort_keys=True, indent=4)

    def sync(self, start: int = None, end: int = None, check_db: bool = True):
        start_time = timer()

        logger.debug(f'Sync job --> Started...')

        # If not done yet, sync genesis block
        if not self.cfg.get('genesis_processed'):
            self.cfg.set('genesis_processed', True)
            self.process_genesis_block()

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
            if not block.exists and not block.number == 0:
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
            if block: self.cfg.set('sync_start', block.number)

        logger.debug(f'Sync job --> Ended after {timer() - start_time:.3f} seconds')

    def get_block(self, block_id: (int, str), check_db: bool = True) -> Block:
        """ 'block' param can either be block hash or block number """

        # Check if block is already in DB
        if check_db:
            if len(str(block_id)) == 64:
                # 'block_id' is Block Hash
                data = self.db.execute(sql.select_block_by_hash(), {'bh': block_id})
            else:
                # 'block_id' is Block Number
                data = self.db.execute(sql.select_block_by_num(), {'bn': block_id})

            if data:
                logger.debug(f'Retrieved block {block_id} from database')
                return Block(data[0][2], exists=True)

        # Retrieve from web
        for source in self.cfg.get('retrieve_from'):
            host = source['host']
            wait = source['wait']

            host = host.replace('{block}', str(block_id))
            logger.debug(f'Retrieving block {block_id} from {host}')

            if wait:
                logger.debug(f'Waiting for {wait} seconds...')
                time.sleep(wait)

            try:

                # Get block from web
                with r.get(host) as data:
                    logger.info(f'Block {block_id} --> {data.text}')

                    if 'error' in data.json():
                        # Block Service does not know block
                        logger.debug(f'Block {block_id} unknown - trying next host...')
                        continue

                    return Block(data.json())

            except (ConnectionError, HTTPError, Timeout, TooManyRedirects, RequestException) as e:
                logger.error(f'Can not retrieve Block {block_id}: {repr(e)}')
                self.tgb.send(f'‼️ Block Sync Error: {e}')

        logger.error(f'Block {block_id} could not be retrieved! Tried all hosts.')
        self.tgb.send(f'‼️ Block Sync Error: No host able to deliver block')
