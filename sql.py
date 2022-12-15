def create_addresses():
    return """
    CREATE TABLE IF NOT EXISTS addresses (
      address text NOT NULL PRIMARY KEY,
      created TIMESTAMP NOT NULL DEFAULT now()
    )
    """


def create_rewards():
    return """
    CREATE TABLE IF NOT EXISTS rewards (
      block_num SERIAL REFERENCES blocks (block_num),
      key text NOT NULL,
      value jsonb NOT NULL,
      reward jsonb NOT NULL,
      created TIMESTAMP NOT NULL DEFAULT now()
    )
    """


def create_contracts():
    return """
    CREATE TABLE IF NOT EXISTS contracts (
      tx_hash text NOT NULL REFERENCES transactions (hash),
      name text NOT NULL PRIMARY KEY,
      code text NOT NULL,
      lst001 BOOLEAN NOT NULL,
      lst002 BOOLEAN NOT NULL,
      lst003 BOOLEAN NOT NULL,
      created TIMESTAMP NOT NULL DEFAULT now()
    )
    """


def create_current_state():
    return """
    CREATE TABLE IF NOT EXISTS current_state (
      tx_hash text NOT NULL REFERENCES transactions (hash),
      key text NOT NULL PRIMARY KEY,
      value jsonb,
      updated TIMESTAMP NOT NULL DEFAULT now(),
      created TIMESTAMP NOT NULL DEFAULT now()
    )
    """


def create_state_change():
    return """
    CREATE TABLE IF NOT EXISTS state_change (
      tx_hash text NOT NULL PRIMARY KEY REFERENCES transactions (hash),
      state jsonb NOT NULL,
      created TIMESTAMP NOT NULL DEFAULT now()
    )
    """


def create_transactions():
    return """
    CREATE TABLE IF NOT EXISTS transactions (
      hash text NOT NULL PRIMARY KEY,
      transaction JSONB NOT NULL,
      block_num SERIAL REFERENCES blocks (block_num),
      created TIMESTAMP NOT NULL DEFAULT now()
    )
    """


def create_missing_blocks():
    return """
    CREATE TABLE IF NOT EXISTS blocks_missing (
      block_num SERIAL NOT NULL PRIMARY KEY,
      created TIMESTAMP NOT NULL DEFAULT now()
    )
    """


def create_invalid_blocks():
    return """
    CREATE TABLE IF NOT EXISTS blocks_invalid (
      block_num SERIAL NOT NULL PRIMARY KEY,
      created TIMESTAMP NOT NULL DEFAULT now()
    )
    """


def create_blocks():
    return """
    CREATE TABLE IF NOT EXISTS blocks (
      block_num SERIAL NOT NULL PRIMARY KEY,
      block JSONB NOT NULL,
      created TIMESTAMP NOT NULL DEFAULT now()
    )
    """


def select_db_size():
    return """
    SELECT pg_size_pretty(pg_database_size(%(n)s))
    """


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


# TODO: Rework to use params like all other statements
def select_balance(address: str, contract: str = None):
    return f"SELECT (CASE WHEN value ? '__fixed__' THEN (value->>'__fixed__')::jsonb ELSE value END) " \
           f"FROM current_state WHERE key LIKE '{contract}.balances:{address}'"


# TODO: Rework to use params like all other statements
def select_balances(address: str):
    return f"SELECT substring(key from 0 for position('.' in key)), " \
           f"(CASE WHEN value ? '__fixed__' THEN (value->>'__fixed__')::jsonb ELSE value END) " \
           f"FROM current_state " \
           f"WHERE key LIKE '%balances:{address}' AND (value->>'__fixed__')::decimal != 0" \
           f"ORDER BY key"


def select_missing_blocks():
    return """
    SELECT block_num
    FROM blocks_missing
    """


def select_invalid_blocks():
    return "SELECT block_num FROM blocks_invalid"


def select_contract():
    return """
    SELECT json_build_object(
      'name', c.name,
      'tx_hash', c.tx_hash,
      'lst001', c.lst001,
      'lst002', c.lst002,
      'lst003', c.lst003,
      'code', c.code
    )
    FROM contracts c
    WHERE name = %(c)s
    """


def select_contracts():
    return """
    SELECT json_build_object(
      'name', c.name,
      'tx_hash', c.tx_hash,
      'lst001', c.lst001,
      'lst002', c.lst002,
      'lst003', c.lst003,
      'code', c.code
    )
    FROM contracts c
    WHERE name LIKE '%(n)s%'
    """


# TODO
def select_state(key: str):
    return f"SELECT value FROM current_state WHERE key = {key}"


# TODO: Needed?
def select_contract_state():
    return """
    SELECT key, value
    FROM current_state
    WHERE key LIKE %(l)s
    """


def insert_invalid_blocks():
    return """
    INSERT INTO blocks_invalid(block_num)
    VALUES (%(bn)s)
    ON CONFLICT (block_num) DO NOTHING
    """


def insert_missing_blocks():
    return """
    INSERT INTO blocks_missing(block_num)
    VALUES (%(bn)s)
    ON CONFLICT (block_num) DO NOTHING
    """


def insert_block():
    return """
    INSERT INTO blocks(block_num, block)
    VALUES (%(bn)s, %(b)s)
    ON CONFLICT (block_num) DO UPDATE SET block = %(b)s
    """


def insert_transaction():
    return """
    INSERT INTO transactions(hash, transaction, block_num)
    VALUES (%(h)s, %(t)s, %(bn)s)
    ON CONFLICT (hash) DO UPDATE SET transaction = %(t)s, block_num = %(bn)s
    """


def insert_contract():
    return """
    INSERT INTO contracts(tx_hash, name, code, lst001, lst002, lst003)
    VALUES (%(txh)s, %(n)s, %(c)s, %(l1)s, %(l2)s, %(l3)s)
    ON CONFLICT (name) DO UPDATE SET tx_hash = %(txh)s, code = %(c)s, lst001 = %(l1)s, lst002 = %(l2)s, lst003 = %(l3)s
    """


def insert_address():
    return """
    INSERT INTO addresses(address)
    VALUES (%(a)s)
    ON CONFLICT (address) DO NOTHING
    """


def insert_reward():
    return """
    INSERT INTO rewards(block_num, key, value, reward)
    VALUES (%(bn)s, %(k)s, %(v)s, %(r)s)
    ON CONFLICT (block_num) DO NOTHING
    """


def insert_state_change():
    return """
    INSERT INTO state_change(tx_hash, state)
    VALUES (%(txh)s, %(s)s)
    ON CONFLICT (tx_hash) DO UPDATE SET state = %(s)s
    """


def insert_current_state():
    return """
    INSERT INTO current_state(tx_hash, key, value)
    VALUES (%(txh)s, %(k)s, %(v)s)
    ON CONFLICT (key) DO UPDATE SET tx_hash = %(txh)s, value = %(v)s
    """


def delete_missing_blocks():
    return """
    DELETE FROM blocks_missing
    WHERE block_num = %(bn)s
    """


def block_exists():
    return """
    SELECT exists (
      SELECT 1
      FROM blocks
      WHERE block_num = %(bn)s LIMIT 1
    )
    """
