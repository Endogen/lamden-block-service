import os
import json
import time

import utils
import shutil
import requests as r

from pathlib import Path
from config import Config
from database import DB
from loguru import logger
from urllib.parse import urlparse
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
        self.save_transaction_in_db(block)
        self.save_state_change_in_db(block)
        self.save_current_state_in_db(block)
        self.save_contract_in_db(block)
        self.save_address_in_db(block)

        if self.cfg.get('save_blocks_to_file') and not self.cfg.get('sync_from_file'):
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
        self.db.execute('blocks_insert', {'bn': content['number'], 'b': json.dumps(content)})
        logger.debug(f'Saved block {content["number"]} in database')

    def save_transaction_in_db(self, content: dict):
        content_without_state = dict(content)
        if 'state' in content_without_state:
            del content_without_state['state']

        for subblock in content_without_state['subblocks']:
            for tx in subblock['transactions']:
                self.db.execute(
                    'transactions_insert',
                    {'h': tx['hash'], 't': json.dumps(tx), 'b': content['number']})

                logger.debug(f'Saved transaction {tx["hash"]} in database')

    def save_state_change_in_db(self, content: dict):
        for subblock in content['subblocks']:
            for tx in subblock['transactions']:
                if tx['status'] == 1:
                    continue
                if 'state' in tx:
                    self.db.execute(
                        'state_change_insert',
                        {'txh': tx['hash'], 's': json.dumps(tx['state'])})

                    logger.debug(f'Saved state change from {tx["hash"]} in database')
                else:
                    logger.debug(f'State change: No state in tx {tx["hash"]}')

    def save_current_state_in_db(self, content: dict):
        for subblock in content['subblocks']:
            for tx in subblock['transactions']:
                if tx['status'] == 1:
                    continue
                if 'state' in tx:
                    for kv in tx['state']:
                        self.db.execute(
                            'current_state_insert',
                            {'txh': tx['hash'], 'k': kv['key'], 'v': json.dumps(kv['value'])})

                    logger.debug(f'Saved current state from {tx["hash"]} in database')
                else:
                    logger.debug(f'Current state: No state in tx {tx["hash"]}')

    def save_contract_in_db(self, content: dict):
        for subblock in content['subblocks']:
            for tx in subblock['transactions']:
                if tx['status'] == 1:
                    continue

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
                        'contracts_insert',
                        {'txh': tx['hash'], 'n': name, 'c': code, 'l1': lst1, 'l2': lst2, 'l3': lst3})

                    logger.debug(f'Saved contract {name} in database')

    def save_address_in_db(self, content: dict):
        for subblock in content['subblocks']:
            for tx in subblock['transactions']:
                if tx['status'] == 1:
                    continue

                pld = tx['transaction']['payload']
                sender = pld['sender']

                if utils.is_valid_address(sender):
                    self.db.execute('addresses_insert', {'a': sender})
                    logger.debug(f'Saving address in database: {sender}')

                if 'kwargs' in pld:
                    if 'to' in pld['kwargs']:
                        to = pld['kwargs']['to']
                        if utils.is_valid_address(to):
                            self.db.execute('addresses_insert', {'a': to})
                            logger.debug(f'Saving address in database: {to}')

    def sync(self, start: int = None, end: int = None):
        start_time = timer()

        logger.debug(f'Sync job --> Started...')

        start = start if start else self.cfg.get('block_current')
        end = end if end else self.cfg.get('block_latest')

        if start == 0:
            if self.download_blocks(self.cfg.get('block_archive')):
                self.cfg.set('sync_from_file', True)

        to_sync = list(range(start + 1, end + 1))
        missing = self.db.execute('blocks_missing_select')
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
            if self.db.execute('block_exists', {'bn': block_num})[0][0]:
                logger.debug(f'Block {block_num} exists - skipping...')
                continue

            if self.cfg.get('sync_from_file'):
                state, block = self.get_block_from_file(block_num)
            else:
                state, block = self.get_block(block_num)

            if block_num in missing and state != State.MISSING:
                self.db.execute('blocks_missing_delete', {'bn': block_num})

            if state == State.OK:
                self.process(block)
            elif state == State.MISSING:
                self.db.execute('blocks_missing_insert', {'bn': block_num})
                logger.warning(f'Block {block_num} missing...')
            elif state == State.INVALID:
                self.db.execute('blocks_invalid_insert', {'bn': block_num})
                logger.warning(f'Block {block_num} invalid...')

            self.cfg.set('block_current', block_num)

        self.cfg.set('block_current', end)
        if self.cfg.get('sync_from_file'): self.cfg.set('sync_from_file', False)
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

    def get_block_from_file(self, block_num: int) -> (State, dict):
        path = os.path.join(self.cfg.get('block_dir'), f'{block_num}.json')
        logger.debug(f'Retrieving block from {path}')

        if not Path(path).is_file():
            logger.warning(f'Missing block {block_num}')
            return State.MISSING, None

        with open(path) as f:
            block = json.load(f)

            logger.debug(f'Block {block_num} --> {block}')
            return self.get_block_state(block)

    def get_block_state(self, block: dict) -> (State, dict):
        if 'error' in block:
            logger.warning(f'Invalid block!')
            return State.INVALID, block
        if block['hash'] == 'block-does-not-exist':
            logger.warning(f'Invalid block!')
            return State.INVALID, block
        return State.OK, block

    def download_blocks(self, url: str):
        if not url:
            logger.debug(f'Skipping downloading blocks - No URL')
            return False

        try:
            start_time = timer()
            logger.debug(f'Downloading blocks from: {url}')

            filename = os.path.basename(urlparse(url).path)

            with r.get(url, stream=True) as req:
                with open(filename, 'wb') as f:
                    shutil.copyfileobj(req.raw, f)

            logger.debug(f'Downloading blocks finished in {timer() - start_time} seconds')

            start_time = timer()
            logger.debug(f'Unzipping block archive: {filename}')

            shutil.unpack_archive(filename=filename, extract_dir=self.cfg.get('block_dir'))
            logger.debug(f'Unzipping block archive finished in {timer() - start_time} seconds')
        except Exception as e:
            logger.exception(f'Downloading blocks failed: {e}')
            return False

        return True

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
