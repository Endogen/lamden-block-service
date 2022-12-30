class WrongBlockDataException(Exception):
    pass


class InvalidBlockException(Exception):
    pass


class Block:

    _content = dict()
    _hash = str()
    _timestamp = str()
    _prev = str()
    _block_num = -1
    _tx = dict()
    _is_valid = False
    _result = str()
    _state = dict()
    _rewards = dict()
    _is_contract = False
    _contract = str()
    _code = str()
    _is_lst001 = False
    _is_lst002 = False
    _is_lst003 = False
    _addresses = list()

    def __init__(self, content: dict):
        if 'error' in content:
            raise InvalidBlockException(content['error'])

        try:
            # Whole block content
            self._content = content

            # Block hash
            self._hash = content['hash']

            # HLC timestamp
            self._timestamp = content['hlc_timestamp']

            # Block number
            self._block_num = int(content['number'])

            # Previous block hash
            self._prev = content['previous']

            # It's the genesis block
            if self._block_num == 0:
                return

            # Save transaction
            self._tx = content['processed']

            # Check for state in transaction
            if 'state' in content['processed']:
                # If present, save as state
                self._state = content['processed']['state']

                # Remove state from transaction itself
                del self._tx['state']

            # Transaction was valid or not
            status = content['processed']['status']
            self._is_valid = True if status == 0 else False

            # If transaction is not valid then 'result' has the error msg
            result = content['processed']['result']
            self._result = None if result == "None" else result

            # Distributed rewards
            self._rewards = content['rewards']

            # Transaction payload
            pld = content['processed']['transaction']['payload']
            con = pld['contract']
            fun = pld['function']

            # Check FROM address
            if self._is_valid_address(pld['sender']):
                self._addresses.append(pld['sender'])
            # Check TO address
            if 'to' in pld['kwargs']:
                if self._is_valid_address(pld['kwargs']['to']):
                    self._addresses.append(pld['kwargs']['to'])

            # Check if new contract was submitted
            if con == 'submission' and fun == 'submit_contract':
                kwargs = pld['kwargs']

                self._is_contract = True
                self._contract = kwargs['name']
                self._code = kwargs['code']

                self._is_lst001 = self._con_is_lst001(kwargs['code'])
                self._is_lst002 = self._con_is_lst002(kwargs['code'])
                self._is_lst003 = self._con_is_lst003(kwargs['code'])

        except Exception as e:
            raise WrongBlockDataException(repr(e))

    @property
    def content(self) -> dict:
        return self._content

    @property
    def hash(self) -> str:
        return self._hash

    @property
    def block_num(self) -> int:
        return self._block_num

    @property
    def timestamp(self) -> str:
        return self._timestamp

    @property
    def prev(self) -> str:
        return self._prev

    @property
    def tx(self) -> dict:
        return self._tx

    @property
    def is_valid(self) -> bool:
        return self._is_valid

    @property
    def result(self) -> str:
        return self._result

    @property
    def state(self) -> dict:
        return self._state

    @property
    def rewards(self) -> dict:
        return self._rewards

    @property
    def is_contract(self) -> bool:
        return self._is_contract

    @property
    def contract(self) -> str:
        return self._contract

    @property
    def code(self) -> str:
        return self._code

    @property
    def is_lst001(self) -> bool:
        return self._is_lst001

    @property
    def is_lst002(self) -> bool:
        return self.is_lst002

    @property
    def is_lst003(self) -> bool:
        return self.is_lst003

    @property
    def addresses(self) -> list:
        return self._addresses

    def _con_is_lst001(self, code: str) -> bool:
        code = code.replace(' ', '')

        if 'balances=Hash(' not in code:
            return False
        if '@export\ndeftransfer(amount:float,to:str):' not in code:
            return False
        if '@export\ndefapprove(amount:float,to:str):' not in code:
            return False
        if '@export\ndeftransfer_from(amount:float,to:str,main_account:str):' not in code:
            return False

        return True

    def _con_is_lst002(self, code: str) -> bool:
        code = code.replace(' ', '')

        if 'metadata=Hash(' not in code:
            return False

        return True

    def _con_is_lst003(self, code: str) -> bool:
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

        return True

    def _is_valid_address(self, address: str) -> bool:
        if not address:
            return False
        if not len(address) == 64:
            return False
        try:
            int(address, 16)
        except:
            return False
        return True
