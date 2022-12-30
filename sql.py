def create_addresses():
    return """
    CREATE TABLE IF NOT EXISTS addresses (
      block_num BIGINT NOT NULL REFERENCES blocks (number),
      address text NOT NULL PRIMARY KEY,
      created TIMESTAMP NOT NULL
    )
    """


def create_rewards():
    return """
    CREATE TABLE IF NOT EXISTS rewards (
      block_num BIGINT NOT NULL REFERENCES blocks (number),
      key text NOT NULL,
      value jsonb NOT NULL,
      reward jsonb NOT NULL,
      created TIMESTAMP NOT NULL,
      PRIMARY KEY (block_num, key)
    )
    """


def create_contracts():
    return """
    CREATE TABLE IF NOT EXISTS contracts (
      block_num BIGINT NOT NULL REFERENCES blocks (number),
      name text NOT NULL PRIMARY KEY,
      code text NOT NULL,
      lst001 BOOLEAN NOT NULL,
      lst002 BOOLEAN NOT NULL,
      lst003 BOOLEAN NOT NULL,
      created TIMESTAMP NOT NULL
    )
    """


def create_current_state():
    return """
    CREATE TABLE IF NOT EXISTS current_state (
      block_num BIGINT NOT NULL REFERENCES blocks (number),
      key text NOT NULL PRIMARY KEY,
      value jsonb NOT NULL,
      updated TIMESTAMP NOT NULL,
      created TIMESTAMP NOT NULL
    )
    """


def create_state_change():
    return """
    CREATE TABLE IF NOT EXISTS state_change (
      block_num BIGINT NOT NULL PRIMARY KEY REFERENCES blocks (number),
      state jsonb NOT NULL,
      created TIMESTAMP NOT NULL
    )
    """


def create_transactions():
    return """
    CREATE TABLE IF NOT EXISTS transactions (
      block_num BIGINT NOT NULL REFERENCES blocks (number),
      hash text NOT NULL PRIMARY KEY,
      transaction JSONB NOT NULL,
      created TIMESTAMP NOT NULL
    )
    """


def create_blocks():
    return """
    CREATE TABLE IF NOT EXISTS blocks (
      number BIGINT NOT NULL PRIMARY KEY,
      hash text NOT NULL,
      block JSONB NOT NULL,
      created TIMESTAMP NOT NULL
    )
    """


def select_db_size():
    return """
    SELECT pg_size_pretty(pg_database_size(%(n)s))
    """


# TODO: Rework to use params like all other statements
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


def select_block_by_num():
    return f"SELECT * FROM blocks WHERE number = %(bn)s"


def select_block_by_hash():
    return f"SELECT * FROM blocks WHERE hash = %(bh)s"


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


def select_state_change():
    return """
    SELECT *
    FROM state_change
    WHERE block_num = %(bn)s
    """


def select_current_state():
    return """
    SELECT *
    FROM current_state
    WHERE key LIKE %(k)s
    """


def insert_block():
    return """
    INSERT INTO blocks(number, hash, block, created)
    VALUES (%(n)s, %(h)s, %(b)s, %(cr)s)
    ON CONFLICT (number) DO UPDATE SET hash = %(h)s, block = %(b)s, created = %(cr)s
    """


def insert_transaction():
    return """
    INSERT INTO transactions(block_num, hash, transaction, created)
    VALUES (%(bn)s, %(h)s, %(t)s, %(cr)s)
    ON CONFLICT (hash) DO UPDATE SET block_num = %(bn)s, transaction = %(t)s, created = %(cr)s
    """


def insert_contract():
    return """
    INSERT INTO contracts(block_num, name, code, lst001, lst002, lst003, created)
    VALUES (%(bn)s, %(n)s, %(c)s, %(l1)s, %(l2)s, %(l3)s, %(cr)s)
    ON CONFLICT (name) DO UPDATE SET block_num = %(bn)s, code = %(c)s, lst001 = %(l1)s, lst002 = %(l2)s, lst003 = %(l3)s, created = %(cr)s
    """


def insert_address():
    return """
    INSERT INTO addresses(block_num, address, created)
    VALUES (%(bn)s, %(a)s, %(cr)s)
    ON CONFLICT (address) DO NOTHING
    """


def insert_reward():
    return """
    INSERT INTO rewards(block_num, key, value, reward, created)
    VALUES (%(bn)s, %(k)s, %(v)s, %(r)s, %(cr)s)
    ON CONFLICT (block_num, key) DO UPDATE SET block_num = %(bn)s, key = %(k)s, value = %(v)s, reward = %(r)s, created = %(cr)s
    """


def insert_state_change():
    return """
    INSERT INTO state_change(block_num, state, created)
    VALUES (%(bn)s, %(s)s, %(cr)s)
    ON CONFLICT (block_num) DO UPDATE SET state = %(s)s, created = %(cr)s
    """


def insert_current_state():
    return """
    INSERT INTO current_state(block_num, key, value, created, updated)
    VALUES (%(bn)s, %(k)s, %(v)s, %(cr)s, %(up)s)
    ON CONFLICT (key) DO UPDATE SET block_num = %(bn)s, value = %(v)s, updated = %(up)s
    """
