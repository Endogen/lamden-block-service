import os
import sys
import uvicorn
import utils
import sql

from pathlib import Path
from config import Config
from database import DB
from loguru import logger
from fastapi import FastAPI
from datetime import timedelta
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from timeit import default_timer as timer

# TODO: Search contract code contains
# TODO: Total burned amount for token
# TODO: API for total stamps used for address
# TODO: API for 'network involvement' of an address - how much other addresses transacted with address?
# TODO: Use similar API as default Block Service
# TODO: Add API for 'TAU forever lost'
# TODO: Add API for address Toplist (TAU or any other token)
# TODO: API to subscribe to state changes
# TODO: API for which contract holds which funds
# TODO: https://stackoverflow.com/questions/1237725/copying-postgresql-database-to-another-server
# TODO: https://github.com/ultrajson/ultrajson

app = FastAPI(title='BlockJuggler API')

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True
)

cfg = Config(os.path.join('cfg', 'api.json'))
db = DB(cfg)

logger.remove()

logger.add(
    sys.stderr,
    level=cfg.get('log_level'))

logger.add(
    os.path.join('log', 'api_{time}.log'),
    retention=timedelta(days=cfg.get('log_retention')),
    format='{time} {level} {name} {message}',
    level=cfg.get('log_level'),
    rotation='10 MB',
    diagnose=True)


@app.get("/", response_class=HTMLResponse)
def root():
    with open(Path('res', 'index.html'), 'r', encoding='utf-8') as f:
        return f.read().replace('\n', '')


@app.get("/db-size")
def db_size():
    start = timer()

    try:
        logger.debug(f'API ENTRY: db_size()')
        result = db.execute_raw(sql.db_size(cfg.get('db_name')))
        logger.debug(f'API RESULT after {timer() - start:.4f} seconds: {result}')
        return result[0][1]
    except Exception as e:
        return {'error': str(e)}


# TODO: Split into 'addresses' and 'contracts' and sort by balance and show balance
@app.get("/holders/{contract}")
def holders(contract: str, addresses: bool = True, contracts: bool = True, top: int = 0):
    logger.debug(f'API ENTRY: holders({contract}, {addresses}, {contracts}, {top})')
    result = db.execute_raw(sql)
    logger.debug(f'API RESULT: {result}')

    holder_list = list()
    for holder in result:
        holder_split = holder[0].split(':')
        if len(holder_split) == 2:
            holder_list.append(holder_split[1])

    return holder_list


@app.get("/balance/{address}")
def balance(address: str, contract: str = None):
    logger.debug(f'API ENTRY: balance({address}, {contract})')

    if contract:
        result = db.execute('balance_select', {'a': f'{contract}.balances:{address}'})
        logger.debug(f'API RESULT: {result}')

        amount = utils.unwrap_fixed(result[0][0])
        return float(amount)
    else:
        result = db.execute('balances_select', {'a': f'%balances:{address}'})
        logger.debug(f'API RESULT: {result}')

        tokens = dict()

        for token in result:
            contract = token[0].split('.')[0]
            amount = utils.unwrap_fixed(token[1])

            if float(amount) == 0:
                continue
            else:
                tokens[contract] = amount

        return tokens


@app.get("/state")
def state(contract: str = None):
    logger.debug(f'API ENTRY: state({contract})')

    if contract:
        result = db.execute('state_contract_select', {'l': f'{contract}'})
        logger.debug(f'API RESULT: {result}')
        # TODO

    else:
        result = db.execute('state_select')
        logger.debug(f'API RESULT: {result}')


@app.get("/contract/{contract}")
def contract(contract: str):
    logger.debug(f'API ENTRY: state({contract})')
    result = db.execute('contract_select', {'c': f'{contract}'})
    logger.debug(f'API RESULT: {result}')
    return result[0][0]


@app.get("/contracts")
def contracts(contract: str = None, lst001: str = None, lst002: str = None):
    logger.debug(f'API ENTRY: state({contract}, {lst001}, {lst002})')
    result = db.execute('contracts_select')
    logger.debug(f'API RESULT: {result}')
    return result


uvicorn.run(app, host=cfg.get('api_host'), port=cfg.get('api_port'))
