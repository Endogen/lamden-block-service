import os
import json
import time
import utils
import requests as r

from config import Config
from database import DB
from loguru import logger
from timeit import default_timer as timer


class BlockState:

    MISSING = 1
    INVALID = 2
    OK = 3


# TODO: When sync from scratch, how do i get invalid blocks into DB?
# TODO: If current block is 0 (meaning we are just starting to sync for first time), download blocks from GitHub
class Block:

    cfg = None
    db = None

    def __init__(self, config: Config, database: DB):
        self.cfg = config
        self.db = database

    def process(self, content: dict):
        start_time = timer()

        self.save_block_in_db(content)
        self.save_transaction_in_db(content)
        self.save_state_change_in_db(content)
        self.save_current_state_in_db(content)
        self.save_contract_in_db(content)
        self.save_address_in_db(content)

        if self.cfg.get('save_to_dir'):
            self.save_block_in_file(content)

        logger.debug(f'Processed block {content["number"]} in {timer() - start_time} seconds')

    def save_block_in_file(self, content: dict):
        block_dir = self.cfg.get('save_to_dir')
        block_num = content['number']

        file = os.path.join(block_dir, f'{block_num}.json')
        os.makedirs(os.path.dirname(file), exist_ok=True)

        with open(file, 'w', encoding='utf-8') as f:
            json.dump(content, f, sort_keys=True, indent=4)
            logger.debug(f'Saved block {block_num} in file')

    def save_block_in_db(self, content: dict):
        self.db.execute('blocks_insert', {'bn': content['number'], 'b': json.dumps(content)})
        logger.debug(f'Saved block {content["number"]} in database')

    def save_transaction_in_db(self, content: dict):
        for subblock in self._get_block_without_state(content)['subblocks']:
            for tx in subblock['transactions']:
                self.db.execute(
                    'transactions_insert',
                    {'h': tx['hash'], 't': json.dumps(tx), 'b': content['number']})

                logger.debug(f'Saved transaction {tx["hash"]} in database')

    def save_state_change_in_db(self, content: dict):
        for subblock in content['subblocks']:
            for tx in subblock['transactions']:
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
                if 'state' in tx:
                    for kv in tx['state']:
                        key = kv['key']
                        value = kv['value']

                        if type(value) is dict:
                            value = next(iter(value.values()))

                        self.db.execute(
                            'current_state_insert',
                            {'txh': tx['hash'], 'k': key, 'v': value, 's': json.dumps(kv)})

                    logger.debug(f'Saved current state from {tx["hash"]} in database')
                else:
                    logger.debug(f'Current state: No state in tx {tx["hash"]}')

    def save_contract_in_db(self, content: dict):
        for subblock in content['subblocks']:
            for tx in subblock['transactions']:
                pld = tx['transaction']['payload']
                con = pld['contract']
                fun = pld['function']

                if con == 'submission' and fun == 'submit_contract':
                    kwargs = pld['kwargs']
                    code = kwargs['code']
                    name = kwargs['name']

                    lst1 = self.is_lst001(code)
                    lst2 = self.is_lst002(code)

                    self.db.execute(
                        'contracts_insert',
                        {'txh': tx['hash'], 'n': name, 'c': code, 'l1': lst1, 'l2': lst2})

                    logger.debug(f'Saved contract {name} in database')

    def save_address_in_db(self, content: dict):
        for subblock in content['subblocks']:
            for tx in subblock['transactions']:
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

        to_sync = list(range(start + 1, end + 1))
        missing = self.db.execute('blocks_missing_select')
        missing = [x[0] for x in missing]

        self.db.execute('blocks_missing_delete')

        to_sync.extend(missing)
        to_sync = list(set(to_sync))
        to_sync.sort(key=int)

        if not to_sync:
            logger.debug(f'Sync job --> Synchronized!')
            return

        logger.debug(f'Missing: {missing}')
        logger.debug(f'To Sync: {to_sync}')

        sleep_for = self.cfg.get('block_sync_wait')

        for block_num in to_sync:
            time.sleep(sleep_for)

            state, block = self.get_block(block_num)

            if state == BlockState.OK:
                self.process(block)
            elif state == BlockState.MISSING:
                self.db.execute('blocks_missing_insert', {'bn': block_num})
                logger.warning(f'Block {block_num} missing...')
            elif state == BlockState.INVALID:
                self.db.execute('blocks_invalid_insert', {'bn': block_num})
                logger.warning(f'Block {block_num} invalid...')

            self.cfg.set('block_current', block_num)

        logger.debug(f'Sync job --> Ended after {timer() - start_time} seconds')

    def get_block(self, block_num: int) -> (BlockState, dict):
        for source in self.cfg.get('retrieve_blocks_from'):
            source = source.replace('{block_num}', str(block_num))
            logger.debug(f'Retrieving block from {source}')

            try:
                with r.get(source) as data:
                    logger.debug(f'Block {block_num} --> {data.text}')

                    block = data.json()

                    if 'error' in block:
                        logger.warning(f'Invalid block {block_num}')
                        return BlockState.INVALID, block
                    if block['hash'] == 'block-does-not-exist':
                        logger.warning(f'Invalid block {block_num}')
                        return BlockState.INVALID, block

                    return BlockState.OK, block
            except Exception as e:
                logger.exception(f'get_block({block_num}) --> {e}')

        logger.error(f'Could not retrieve block {block_num}!')
        return BlockState.MISSING, None

    def _get_block_without_state(self, d: dict) -> dict:
        new_d = dict(d)

        if 'state' in new_d:
            del new_d['state']

        return new_d

    def is_lst001(self, code: str) -> bool:
        code = code.replace(' ', '')

        if 'balances=Hash(' not in code:
            logger.debug(f'Contract does not comply with LST001: balances')
            return False
        if '@export\ndeftransfer(amount:float,to:str)' not in code:
            logger.debug(f'Contract does not comply with LST001: transfer')
            return False
        if '@export\ndefapprove(amount:float,to:str)' not in code:
            logger.debug(f'Contract does not comply with LST001: approve')
            return False
        if '@export\ndeftransfer_from(amount:float,to:str,main_account:str)' not in code:
            logger.debug(f'Contract does not comply with LST001: transfer_from')
            return False

        return True

    def is_lst002(self, code: str) -> bool:
        code = code.replace(' ', '')

        if 'metadata=Hash(' not in code:
            logger.debug(f'Contract does not comply with LST002: metadata')
            return False

        return True
