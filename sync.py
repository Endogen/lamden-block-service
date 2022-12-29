import os
import sql
import json
import time

import requests as r

from config import Config
from database import DB
from loguru import logger
from timeit import default_timer as timer
from block import Block, WrongBlockDataException, InvalidBlockException


class Sync:
    cfg = None
    db = None

    def __init__(self, config: Config, database: DB):
        self.cfg = config
        self.db = database

        # Save block hash of genesis block
        if not self.cfg.get('genesis_block_hash'):
            self.cfg.set('genesis_block_hash', self.get_block(0).hash)

    def process(self, block: Block):
        start_time = timer()

        # SAVE BLOCK

        self.db.execute(
            sql.insert_block(),
            {'bn': block.block_num, 'b': json.dumps(block.content)})

        logger.debug(f'Saved block {block.block_num} - {timer() - start_time} seconds')

        # SAVE TRANSACTION

        self.db.execute(
            sql.insert_transaction(),
            {'h': block.tx['hash'], 't': json.dumps(block.tx), 'bn': block.block_num})

        logger.debug(f'Saved tx {block.tx["hash"]} - {timer() - start_time} seconds')

        # SAVE STATE

        if block.state:
            if block.is_valid:
                self.db.execute(
                    sql.insert_state_change(),
                    {'txh': block.tx["hash"], 's': json.dumps(block.state)})

                logger.debug(f'Saved state {block.state} - {timer() - start_time} seconds')

                for kv in block.state:
                    self.db.execute(
                        sql.insert_current_state(),
                        {'txh': block.tx["hash"], 'k': kv['key'], 'v': json.dumps(kv['value'])})

                    logger.debug(f'Saved single state {kv["key"]} - {timer() - start_time} seconds')
            else:
                logger.debug(f'State not saved - tx {block.tx["hash"]} invalid')
        else:
            logger.debug(f'No state in tx {block.tx["hash"]}')

        # SAVE CONTRACT

        if block.is_contract:
            self.db.execute(
                sql.insert_contract(),
                {'txh': block.tx["hash"], 'n': block.contract, 'c': block.code,
                 'l1': block.is_lst001, 'l2': block.is_lst002, 'l3': block.is_lst003})

            logger.debug(f'Saved contract {block.contract} '
                         f'(LST001={block.is_lst001}, LST002={block.is_lst002}, LST003={block.is_lst003}) '
                         f'- {timer() - start_time} seconds')

        # SAVE ADDRESSES
        for address in block.addresses:
            self.db.execute(
                sql.insert_address(),
                {'a': address})

            logger.debug(f'Saved address {address} - {timer() - start_time} seconds')

        # SAVE REWARDS

        for rw in block.rewards:
            self.db.execute(
                sql.insert_reward(),
                {'bn': block.block_num, 'k': rw['key'], 'v': json.dumps(rw['value']), 'r': json.dumps(rw['reward'])})

            logger.debug(f'Saved rewards {rw} - {timer() - start_time} seconds')

        # SAVE BLOCK TO FILE

        if self.cfg.get('save_blocks_to_file'):
            self.save_block_in_file(block)

            logger.debug(f'Saved block {block.block_num} to file - {timer() - start_time} seconds')

        logger.debug(f'Processed block {block.block_num} in {timer() - start_time} seconds')

    def save_block_in_file(self, block: Block):
        block_dir = self.cfg.get('block_dir')
        file = os.path.join(block_dir, f'{block.block_num}.json')
        os.makedirs(os.path.dirname(file), exist_ok=True)

        with open(file, 'w', encoding='utf-8') as f:
            json.dump(block.content, f, sort_keys=True, indent=4)
            logger.debug(f'Saved block {block.block_num} to file')

    # TODO: How to make sure that sync from CLI won't set values in config?
    def sync(self, start: int = None, end: int = None, process_existing: bool = False):
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

        block = self.get_block(sync_start)

        while block:
            # Check if block already exists in database
            if self.db.execute(sql.block_exists(), {'bn': block.block_num})[0][0]:
                if process_existing:
                    self.process(block)
                    logger.debug(f'Block {block.block_num} exists - processing...')
                else:
                    # TODO: Read block from DB and set 'sync_start = block.block_num'
                    logger.debug(f'Block {block.block_num} exists - skipping...')
            else:
                self.process(block)

            # End sync if current block number is same as sync end
            if block.block_num == sync_end:
                self.cfg.set('sync_end', sync_start)
                break

            # If previous block is genesis block, process it separately
            # Genesis block is special and differs from normal blocks
            if block.prev == self.cfg.get('genesis_block_hash'):
                # TODO: Process genesis block
                break

            block = self.get_block(block.prev)
            self.cfg.set('sync_start', block.block_num)

        logger.debug(f'Sync job --> Ended after {timer() - start_time} seconds')

    def get_block(self, block: (int, str)) -> Block:
        """ 'block' param can either be block hash or block number """

        for source in self.cfg.get('retrieve_state_from'):
            host = source['host']
            wait = source['wait']

            if wait:
                logger.debug(f'Waiting for {wait} seconds...')
                time.sleep(wait)

            host = host.replace('{block}', str(block))
            logger.debug(f'Retrieving block from {host}')

            try:
                with r.get(host) as data:
                    logger.info(f'Block {block} --> {data.text}')
                    return Block(data.json())

            except InvalidBlockException as e:
                logger.exception(f'Block {block} - invalid: {e}')
            except WrongBlockDataException as e:
                logger.exception(f'Block {block} - wrong data: {e}')
            except Exception as e:
                logger.exception(f'Block {block} - can not retrieve: {e}')
