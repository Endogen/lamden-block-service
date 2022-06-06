def select_db_size(db_name: str):
    return f"SELECT pg_size_pretty(pg_database_size('{db_name}'))"


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


# TODO: Remove balances with '0'
# TODO: return only contract name, not whole string
def select_balances(address: str):
    return f"SELECT key, (CASE WHEN value ? '__fixed__' THEN (value->>'__fixed__')::jsonb ELSE value END) " \
           f"FROM current_state " \
           f"WHERE key LIKE '%balances:{address}' " \
           f"ORDER BY key"


def select_missing_blocks():
    return "SELECT block_num FROM blocks_missing"


def select_invalid_blocks():
    return "SELECT block_num FROM blocks_invalid"
