def db_size(db_name: str):
    return f"SELECT pg_size_pretty(pg_database_size('{db_name}'))"

"""
def holders(contract: str, addresses: bool, contracts: bool, top: int):
    base = f"SELECT key FROM current_state WHERE key LIKE '{contract}.balances:%'"

    if top != 0:
        limit = f" limit {top}"
"""


def select_missing_blocks():
    return 'SELECT block_num FROM blocks_missing'


def select_invalid_blocks():
    return 'SELECT block_num FROM blocks_invalid'
