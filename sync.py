import os
import sql
import json
import time

import requests as r

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

        # Check for genesis block
        if block.number == 0:
            self.process_genesis_block()
            return

        # SAVE BLOCK

        self.db.execute(sql.insert_block(),
            {'n': block.number, 'h': block.hash, 'b': json.dumps(block.content), 'cr': block.timestamp})

        logger.debug(f'Saved block {block.number} - {timer() - start_time} seconds')

        # SAVE TRANSACTION

        self.db.execute(sql.insert_transaction(),
            {'bn': block.number, 'h': block.tx_hash, 't': json.dumps(block.tx), 'cr': block.timestamp})

        logger.debug(f'Saved tx {block.tx_hash} - {timer() - start_time} seconds')

        # SAVE STATE

        if block.state:
            if block.is_valid:
                for kv in block.state:
                    # Check if state is already known and newer than current data
                    data = self.db.execute(sql.select_state(), {'k': kv['key']})
                    if data and data[0][0] > block.number:
                        logger.debug(f'State {kv["key"]} already up to date - {timer() - start_time} seconds')
                        break

                    self.db.execute(sql.insert_state(),
                        {'bn': block.number, 'k': kv['key'], 'v': json.dumps(kv['value']),
                         'cr': block.timestamp, 'up': block.timestamp})

                    logger.debug(f'Saved state {kv} - {timer() - start_time} seconds')
            else:
                logger.debug(f'State not saved - tx {block.tx_hash} invalid')

        # SAVE REWARDS

        for rw in block.rewards:
            if block.is_valid:
                # Save rewards in rewards table
                self.db.execute(sql.insert_reward(),
                    {'bn': block.number, 'k': rw['key'], 'v': json.dumps(rw['value']),
                     'r': json.dumps(rw['reward']), 'cr': block.timestamp})

                logger.debug(f'Saved rewards {rw} - {timer() - start_time} seconds')

                # Save rewards in state table
                self.db.execute(sql.insert_state(),
                    {'bn': block.number, 'k': rw['key'], 'v': json.dumps(rw['value']),
                     'cr': block.timestamp, 'up': block.timestamp})

                logger.debug(f'Saved rewards {rw} - {timer() - start_time} seconds')

        # SAVE CONTRACT

        if block.is_contract:
            self.db.execute(sql.insert_contract(),
                {'bn': block.number, 'n': block.contract, 'c': block.code,
                 'l1': block.is_lst001, 'l2': block.is_lst002, 'l3': block.is_lst003, 'cr': block.timestamp})

            logger.debug(f'Saved contract {block.contract} '
                         f'(LST001={block.is_lst001}, LST002={block.is_lst002}, LST003={block.is_lst003}) '
                         f'- {timer() - start_time} seconds')

        # SAVE ADDRESSES

        for address in block.addresses:
            # Check if address is already known and older than current data
            data = self.db.execute(sql.select_address(), {'a': address})

            if data and data[0][0] < block.number:
                logger.debug(f'Address {address} already present - {timer() - start_time} seconds')
            else:
                self.db.execute(sql.insert_address(),
                    {'bn': block.number, 'a': address, 'cr': block.timestamp})

                logger.debug(f'Saved address {address} - {timer() - start_time} seconds')

        # SAVE BLOCK TO FILE

        if self.cfg.get('save_blocks_to_file'):
            self.save_block_in_file(block)

            logger.debug(f'Saved block {block.number} to file - {timer() - start_time} seconds')

        logger.debug(f'Processed block {block.number} in {timer() - start_time} seconds')

    def process_genesis_block(self):
        # TODO: Process genesis block before anything else
        pass

    def save_block_in_file(self, block: Block):
        block_dir = self.cfg.get('block_dir')
        file = os.path.join(block_dir, f'{block.number}.json')
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

        logger.debug(f'Sync job --> Ended after {timer() - start_time} seconds')

    def get_block(self, block_id: (int, str), check_db: bool = True) -> Block:
        """ 'block' param can either be block hash or block number """

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

        for source in self.cfg.get('retrieve_from'):
            host = source['host']
            wait = source['wait']

            logger.debug(f'Retrieving block {block_id} from {host}')

            if wait:
                logger.debug(f'Waiting for {wait} seconds...')
                time.sleep(wait)

            host = host.replace('{block}', str(block_id))

            try:

                with r.get(host) as data:
                    logger.info(f'Block {block_id} --> {data.text}')
                    return Block(data.json(), source=Source.WEB)

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
