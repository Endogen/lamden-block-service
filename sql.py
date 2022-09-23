def select_db_size(db_name: str):
    return f"SELECT pg_size_pretty(pg_database_size('{db_name}'))"


def create_addresses():
    return "CREATE TABLE IF NOT EXISTS addresses (" \
           "address text NOT NULL PRIMARY KEY," \
           "created TIMESTAMP NOT NULL DEFAULT now())"


def create_contracts():
    return "CREATE TABLE IF NOT EXISTS contracts (" \
           "tx_hash text NOT NULL REFERENCES transactions (hash)," \
           "name text NOT NULL PRIMARY KEY," \
           "code text NOT NULL," \
           "lst001 BOOLEAN NOT NULL," \
           "lst002 BOOLEAN NOT NULL," \
           "lst003 BOOLEAN NOT NULL," \
           "created TIMESTAMP NOT NULL DEFAULT now())"


def create_current_state():
    return "CREATE TABLE IF NOT EXISTS current_state (" \
           "tx_hash text NOT NULL REFERENCES transactions (hash)," \
           "key text NOT NULL PRIMARY KEY," \
           "value jsonb," \
           "updated TIMESTAMP NOT NULL DEFAULT now()," \
           "created TIMESTAMP NOT NULL DEFAULT now())"


def create_state_change():
    return "CREATE TABLE IF NOT EXISTS state_change (" \
           "tx_hash text NOT NULL PRIMARY KEY REFERENCES transactions (hash)," \
           "state jsonb NOT NULL," \
           "created TIMESTAMP NOT NULL DEFAULT now())"


def create_transactions():
    return "CREATE TABLE IF NOT EXISTS transactions (" \
           "hash text NOT NULL PRIMARY KEY," \
           "transaction JSONB NOT NULL," \
           "block SERIAL REFERENCES blocks," \
           "created TIMESTAMP NOT NULL DEFAULT now())"


def create_blocks_missing():
    return "CREATE TABLE IF NOT EXISTS blocks_missing (" \
           "block_num SERIAL NOT NULL PRIMARY KEY," \
           "created TIMESTAMP NOT NULL DEFAULT now())"


def create_blocks_invalid():
    return "CREATE TABLE IF NOT EXISTS blocks_invalid (" \
           "block_num SERIAL NOT NULL PRIMARY KEY," \
           "created TIMESTAMP NOT NULL DEFAULT now())"


def create_blocks():
    return "CREATE TABLE IF NOT EXISTS blocks (" \
           "block_num SERIAL NOT NULL PRIMARY KEY," \
           "block JSONB NOT NULL," \
           "created TIMESTAMP NOT NULL DEFAULT now())"


def select_holders(contract: str, addresses: bool = True, contracts: bool = True, limit: int = 0):
    add_con = top = ''

    if addresses and not contracts:
        add_con = "AND state.clean NOT LIKE 'con_%'"
    if contracts and not addresses:
        add_con = "AND state.clean LIKE 'con_%'"

    if limit != 0:
        top = f"LIMIT {limit}"

    return f"SELECT state.clean, state.val " \
           f"FROM (SELECT REPLACE(key, '{contract}.balances:', '') AS clean, " \
           f"(CASE WHEN value ? '__fixed__' THEN (value->>'__fixed__')::jsonb ELSE value END) AS val " \
           f"FROM current_state WHERE key LIKE '{contract}.balances:%') AS state " \
           f"WHERE state.clean NOT LIKE '%:%' AND state.val::decimal != '0.0' {add_con} " \
           f"ORDER BY state.val DESC " \
           f"{top}"


def select_balance(address: str, contract: str = None):
    return f"SELECT (CASE WHEN value ? '__fixed__' THEN (value->>'__fixed__')::jsonb ELSE value END) " \
           f"FROM current_state WHERE key LIKE '{contract}.balances:{address}'"


def select_balances(address: str):
    return f"SELECT substring(key from 0 for position('.' in key)), " \
           f"(CASE WHEN value ? '__fixed__' THEN (value->>'__fixed__')::jsonb ELSE value END) " \
           f"FROM current_state " \
           f"WHERE key LIKE '%balances:{address}' AND (value->>'__fixed__')::decimal != 0" \
           f"ORDER BY key"


def select_missing_blocks():
    return "SELECT block_num FROM blocks_missing"


def select_invalid_blocks():
    return "SELECT block_num FROM blocks_invalid"


def select_contract(contract: str):
    return f"SELECT json_build_object(" \
           f"'name', c.name," \
           f"'tx_hash', c.tx_hash," \
           f"'lst001', c.lst001," \
           f"'lst002', c.lst002," \
           f"'lst003', c.lst003," \
           f"'code', c.code)" \
           f"FROM contracts c" \
           f"WHERE name = {contract}"


# TODO
def select_contracts(contract: str, lst1: bool, lst2: bool, lst3: bool):
    return f"SELECT json_build_object(" \
           f"'name', c.name," \
           f"'tx_hash', c.tx_hash," \
           f"'lst001', c.lst001," \
           f"'lst002', c.lst002," \
           f"'lst003', c.lst003," \
           f"'code', c.code)" \
           f"FROM contracts c" \
           f"WHERE name LIKE '%{contract}%'"


# TODO
def select_state(key: str):
    return f"SELECT value FROM current_state WHERE key = {key}"


def insert_invalid_blocks(block_num: int):
    return f"INSERT INTO blocks_invalid(block_num) " \
           f"VALUES ({block_num}) " \
           f"ON CONFLICT (block_num) DO NOTHING"


def insert_missing_blocks(block_num: int):
    return f"INSERT INTO blocks_missing(block_num) " \
           f"VALUES ({block_num}) " \
           f"ON CONFLICT (block_num) DO NOTHING"


def insert_block(block_num: int, content: str):
    return f"INSERT INTO blocks(block_num, block) " \
           f"VALUES ({block_num}, {content}) " \
           f"ON CONFLICT (block_num) DO UPDATE SET block = {block_num}"


def insert_transaction(tx_hash: str, content: str, block_num: int):
    return f"INSERT INTO transactions(hash, transaction, block) " \
           f"VALUES ({tx_hash}, {content}, {block_num}) " \
           f"ON CONFLICT (hash) DO UPDATE SET transaction = {content}, block = {block_num}"


def insert_contract(tx_hash: str, name: str, code: str, lst1: bool, lst2: bool, lst3: bool):
    return f"INSERT INTO contracts(tx_hash, name, code, lst001, lst002, lst003) " \
           f"VALUES ({tx_hash}, {name}, {code}, {lst1}, {lst2}, {lst3}) " \
           f"ON CONFLICT (name) DO UPDATE SET tx_hash = {tx_hash}, code = {code}, lst001 = {lst1}, lst002 = {lst2}, lst003 = {lst3}"


def insert_address(address: str):
    return f"INSERT INTO addresses(address) " \
           f"VALUES ({address}) " \
           f"ON CONFLICT (address) DO NOTHING"


def insert_state_change(tx_hash: str, state: str):
    return f"INSERT INTO state_change(tx_hash, state) " \
           f"VALUES ({tx_hash}, {state}) " \
           f"ON CONFLICT (tx_hash) DO UPDATE SET state = {state}"


def insert_current_state(tx_hash: str, key: str, value: str):
    return f"INSERT INTO current_state(tx_hash, key, value) " \
           f"VALUES ({tx_hash}, {key}, {value}) " \
           f"ON CONFLICT (key) DO UPDATE SET tx_hash = {tx_hash}, value = {value}"


def delete_missing_blocks(block_num: int):
    return f"DELETE FROM blocks_missing WHERE block_num = {block_num}"


def block_exists(block_num: int):
    return f"SELECT exists (" \
           f"SELECT 1" \
           f"FROM blocks" \
           f"WHERE block_num = {block_num} LIMIT 1)"
