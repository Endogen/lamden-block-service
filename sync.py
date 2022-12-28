import os
import sql
import json
import time

import requests as r

from block import Block
from config import Config
from database import DB
from loguru import logger
from timeit import default_timer as timer


# TODO: Remove INVALID
class State:
    MISSING = 1
    INVALID = 2
    OK = 3


class Sync:
    cfg = None
    db = None

    def __init__(self, config: Config, database: DB):
        self.cfg = config
        self.db = database

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

    # TODO: Maybe i have to remove MISSING BLOCKS...
    # TODO: If block has_prev is False, load state from genesis_block.json
    def sync(self, start: int = None, end: int = None, include_missing: bool = True, check_existing: bool = True):
        start_time = timer()

        logger.debug(f'Sync job --> Started...')

        current_block = 0
        start_block = start if start else self.cfg.get('block_current')
        end_block = end if end else self.cfg.get('block_synced')

        # TODO: Create block here and replave TRUE check with block.has_prev
        sync = True

        while sync:
            # 1) check if check_existing is true. if yes, check if exists. if yes, extract next block
            # 2) get block, return if it has a previous block
            # 3) set current_block to last checked block?
            # 4) check if previous block is end_block
            # 5)
            # check if we are done yet. If yes, set 'sync' to False
            # add timer to check how long we are syncing. If too long, end and wait for next sync

        # If block_synced == 0, then we need to find the first block nr somehow

        # TODO: After regular sync is over, process genesis block if not done yet

        if include_missing:
            missing = self.db.execute(sql.select_missing_blocks())
            missing = [x[0] for x in missing]


        to_sync = list(range(start + 1, end + 1))


        to_sync.extend(missing)
        to_sync = list(set(to_sync))
        to_sync.sort(key=int)

        if not to_sync:
            logger.debug(f'Sync job --> Synchronized!')
            return

        logger.debug(f'Missing: {missing}')
        logger.debug(f'To Sync: {to_sync}')

        for block_num in to_sync:
            if self.db.execute(sql.block_exists(), {'bn': block_num})[0][0]:
                logger.debug(f'Block {block_num} exists - skipping...')
                continue

            state, block = self.get_block(block_num)

            if block_num in missing and state != State.MISSING:
                self.db.execute(sql.delete_missing_blocks(), {'bn': block_num})

            if state == State.OK:
                self.process(block)
            elif state == State.MISSING:
                self.db.execute(sql.insert_missing_blocks(), {'bn': block_num})
                logger.warning(f'Block {block_num} missing...')
            elif state == State.INVALID:
                self.db.execute(sql.insert_invalid_blocks(), {'bn': block_num})
                logger.warning(f'Block {block_num} invalid...')

            self.cfg.set('block_current', block_num)

        self.cfg.set('block_current', end)
        logger.debug(f'Sync job --> Ended after {timer() - start_time} seconds')

    def get_block(self, block_num: int) -> (State, dict):
        for source in self.cfg.get('retrieve_state_from'):
            host = source['host']
            wait = source['wait']

            if wait:
                logger.debug(f'Waiting for {wait} seconds...')
                time.sleep(wait)

            host = host.replace('{block_num}', str(block_num))
            logger.debug(f'Retrieving block from {host}')

            try:
                with r.get(host) as data:
                    logger.info(f'Block {block_num} --> {data.text}')
                    state, block = self.get_block_state(data.json())

                    if state == State.OK:
                        return state, block

            except Exception as e:
                logger.exception(f'get_block({block_num}) --> {e}')

        logger.error(f'Could not retrieve block {block_num}!')
        return State.INVALID, None

    def get_block_state(self, block: dict) -> (State, dict):
        if 'error' in block:
            logger.warning(f'Invalid block!')
            return State.INVALID, block
        if block['hash'] == 'block-does-not-exist':
            logger.warning(f'Invalid block!')
            return State.INVALID, block
        return State.OK, block
