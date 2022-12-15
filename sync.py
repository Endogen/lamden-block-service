import os
import sql
import json
import time

import utils
import requests as r

from config import Config
from database import DB
from loguru import logger
from timeit import default_timer as timer


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

    def process(self, block: dict):
        start_time = timer()

        self.save_block_in_db(block)
        self.save_tx_in_db(block)
        self.save_state_in_db(block)
        self.save_contract_in_db(block)
        self.save_address_in_db(block)
        self.save_reward_in_db(block)

        if self.cfg.get('save_blocks_to_file'):
            self.save_block_in_file(block)

        logger.debug(f'Processed block {block["number"]} in {timer() - start_time} seconds')

    def save_block_in_file(self, content: dict):
        block_dir = self.cfg.get('block_dir')
        block_num = content['number']

        file = os.path.join(block_dir, f'{block_num}.json')
        os.makedirs(os.path.dirname(file), exist_ok=True)

        with open(file, 'w', encoding='utf-8') as f:
            json.dump(content, f, sort_keys=True, indent=4)
            logger.debug(f'Saved block {block_num} to file')

    def save_block_in_db(self, content: dict):
        self.db.execute(sql.insert_block(), {'bn': content['blockNum'], 'b': json.dumps(content)})
        logger.debug(f'Saved block {content["number"]}')

    def save_tx_in_db(self, content: dict):
        tx_without_state = dict(content['processed'])

        if 'state' in tx_without_state:
            del tx_without_state['state']

        tx = tx_without_state['transaction']

        self.db.execute(
            sql.insert_transaction(),
            {'h': tx['hash'], 't': json.dumps(tx), 'bn': content['number']})

        logger.debug(f'Saved tx {tx["hash"]}')

    def save_state_in_db(self, content: dict):
        tx = content['processed']

        if 'state' in tx:
            if tx['status'] == 1:
                logger.debug(f'State not saved - tx {tx["hash"]} invalid')
                return

            self.db.execute(
                sql.insert_state_change(),
                {'txh': tx['hash'], 's': json.dumps(tx['state'])})

            logger.debug(f'Saved state {tx["state"]}')

            for kv in tx['state']:
                self.db.execute(
                    sql.insert_current_state(),
                    {'txh': tx['hash'], 'k': kv['key'], 'v': json.dumps(kv['value'])})

                logger.debug(f'Saved single state {kv["key"]}')
        else:
            logger.debug(f'No state in tx {tx["hash"]}')

    def save_contract_in_db(self, content: dict):
        tx = content['processed']

        if tx['status'] == 1:
            return

        pld = tx['transaction']['payload']
        con = pld['contract']
        fun = pld['function']

        if con == 'submission' and fun == 'submit_contract':
            kwargs = pld['kwargs']
            code = kwargs['code']
            name = kwargs['name']

            lst1 = self.con_is_lst001(code)
            lst2 = self.con_is_lst002(code)
            lst3 = self.con_is_lst003(code)

            self.db.execute(
                sql.insert_contract(),
                {'txh': tx['hash'], 'n': name, 'c': code, 'l1': lst1, 'l2': lst2, 'l3': lst3})

            logger.debug(f'Saved contract {name}')

    def save_address_in_db(self, content: dict):
        tx = content['processed']

        pld = tx['transaction']['payload']
        sender = pld['sender']

        if utils.is_valid_address(sender):
            self.db.execute(sql.insert_address(), {'a': sender})

            logger.debug(f'Saved address {sender}')

        if 'kwargs' in pld:
            if 'to' in pld['kwargs']:
                to = pld['kwargs']['to']
                if utils.is_valid_address(to):
                    self.db.execute(sql.insert_address(), {'a': to})

                    logger.debug(f'Saved address {to}')

    def save_reward_in_db(self, content: dict):
        rewards = content['rewards']

        for rw in rewards:
            self.db.execute(
                sql.insert_reward(),
                {'bn': content['number'], 'k': rw['key'], 'v': json.dumps(rw['value']), 'r': json.dumps(rw['reward'])})

            logger.debug(f'Saved reward {rw}')

    def sync(self, start: int = None, end: int = None):
        start_time = timer()

        logger.debug(f'Sync job --> Started...')

        start = start if start else self.cfg.get('block_current')
        end = end if end else self.cfg.get('block_latest')

        to_sync = list(range(start + 1, end + 1))
        missing = self.db.execute(sql.select_missing_blocks())
        missing = [x[0] for x in missing]

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

    def con_is_lst001(self, code: str) -> bool:
        code = code.replace(' ', '')

        if 'balances=Hash(' not in code:
            return False
        if '@export\ndeftransfer(amount:float,to:str):' not in code:
            return False
        if '@export\ndefapprove(amount:float,to:str):' not in code:
            return False
        if '@export\ndeftransfer_from(amount:float,to:str,main_account:str):' not in code:
            return False

        logger.debug('Contract is LST001 compatible')
        return True

    def con_is_lst002(self, code: str) -> bool:
        code = code.replace(' ', '')

        if 'metadata=Hash(' not in code:
            return False

        logger.debug('Contract is LST002 compatible')
        return True

    def con_is_lst003(self, code: str) -> bool:
        code = code.replace(' ', '')

        if 'collection_name=Variable()' not in code:
            return False
        if 'collection_owner=Variable()' not in code:
            return False
        if 'collection_nfts=Hash(' not in code:
            return False
        if 'collection_balances=Hash(' not in code:
            return False
        if 'collection_balances_approvals=Hash(' not in code:
            return False
        if '@export\ndefmint_nft(name:str,description:str,ipfs_image_url:str,metadata:dict,amount:int):' not in code:
            return False
        if '@export\ndeftransfer(name:str,amount:int,to:str):' not in code:
            return False
        if '@export\ndefapprove(amount:int,name:str,to:str):' not in code:
            return False
        if '@export\ndeftransfer_from(name:str,amount:int,to:str,main_account:str):' not in code:
            return False

        logger.debug('Contract is LST003 compatible')
        return True
