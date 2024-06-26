class Block:

    def __init__(self, content: dict, exists: bool = False):
        # Whole block content
        self._content = content

        # Block exists in DB?
        self._exists = exists

        # Block hash
        self._hash = content['hash']

        # HLC timestamp
        self._timestamp = content['hlc_timestamp'].replace('Z_0', '')

        # Block number
        self._number = int(content['number'])

        # Previous block hash
        self._prev = content['previous']

        if 'rewards' in content:
            # Distributed rewards to node owners
            self._rewards = content['rewards']
        else:
            self._rewards = list()

        if 'processed' in content:
            # Save transaction
            self._tx = content['processed']

            # Save transaction hash
            self._tx_hash = content['processed']['hash']

            # Check for state in transaction
            if 'state' in content['processed']:
                # If present, save as state
                self._state = content['processed']['state']

                # Remove state from transaction itself
                del self._tx['state']
            else:
                self._state = list()

            # Transaction was valid or not
            status = content['processed']['status']
            self._tx_is_valid = True if status == 0 else False

            # If transaction is not valid then 'result' has the error msg
            result = content['processed']['result']
            self._result = None if result == "None" else result

            # Transaction payload
            pld = content['processed']['transaction']['payload']
            con = pld['contract']
            fun = pld['function']

            self._addresses = set()

            # Check FROM address
            if self.is_valid_address(pld['sender']):
                self._addresses.add(pld['sender'])
                # Save sender address
                self._sender = pld['sender']
            # Check TO address (if it exists)
            if 'kwargs' in pld and 'to' in pld['kwargs']:
                if self.is_valid_address(pld['kwargs']['to']):
                    self._addresses.add(pld['kwargs']['to'])

            # Check if new contract was submitted
            if con == 'submission' and fun == 'submit_contract':
                kwargs = pld['kwargs']

                self._is_new_contract = True
                self._contract = kwargs['name']
                self._code = kwargs['code']

                self._is_lst001 = self.con_is_lst001(kwargs['code'])
                self._is_lst002 = self.con_is_lst002(kwargs['code'])
                self._is_lst003 = self.con_is_lst003(kwargs['code'])
            else:
                self._is_new_contract = False
                self._contract = None
                self._code = None

                self._is_lst001 = False
                self._is_lst002 = False
                self._is_lst003 = False
        else:
            self._tx = None
            self._tx_hash = None
            self._tx_is_valid = False
            self._result = None
            self._state = list()
            self._addresses = set()

            self._is_new_contract = False
            self._contract = None
            self._code = None

            self._is_lst001 = False
            self._is_lst002 = False
            self._is_lst003 = False

    @property
    def exists(self) -> bool:
        return self._exists

    @property
    def content(self) -> dict:
        return self._content

    @property
    def hash(self) -> str:
        return self._hash

    @property
    def number(self) -> int:
        return self._number

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
    def tx_hash(self) -> str:
        return self._tx_hash

    @property
    def tx_is_valid(self) -> bool:
        return self._tx_is_valid

    @property
    def result(self) -> str:
        return self._result

    @property
    def sender(self) -> str:
        return self._sender

    @property
    def state(self) -> list:
        return self._state

    @property
    def rewards(self) -> list:
        return self._rewards

    @property
    def is_new_contract(self) -> bool:
        return self._is_new_contract

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
        return list(self._addresses)

    @staticmethod
    def con_is_lst001(code: str) -> bool:
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

    @staticmethod
    def con_is_lst002(code: str) -> bool:
        code = code.replace(' ', '')

        if 'metadata=Hash(' not in code:
            return False

        return True

    @staticmethod
    def con_is_lst003(code: str) -> bool:
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

    @staticmethod
    def is_valid_address(address: str) -> bool:
        if not address:
            return False
        if not len(address) == 64:
            return False
        try:
            int(address, 16)
        except:
            return False
        return True
