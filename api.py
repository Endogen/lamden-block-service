import os
import sys
import uvicorn
import sql

from pathlib import Path
from config import Config
from database import DB
from loguru import logger
from fastapi import FastAPI
from datetime import timedelta
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from timeit import default_timer as timer
from tgbot import TelegramBot

# TODO: Make all functions async with 'async def'
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

app = FastAPI(title='LAPI')

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True
)

bot = TelegramBot(Config('cfg', 'tgbot.json'))
db = DB(Config('cfg', 'db.json'))
cfg = Config('cfg', 'api.json')

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


# TOOD: Why is the favicon not showing?
@app.get('/favicon.ico', include_in_schema=False)
async def favicon():
    return FileResponse(Path('res', 'favicon.ico'))


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def root():
    with open(Path('res', 'index.html'), 'r', encoding='utf-8') as f:
        return f.read().replace('\n', '')


@app.get("/db-size")
def db_size():
    start = timer()

    try:
        logger.debug(f'API ENTRY: db_size()')
        result = db.execute_raw(sql.select_db_size(cfg.get('db_name')))
        logger.debug(f'API RESULT after {timer() - start:.4f} seconds: {result}')

        return result[0][1]

    except Exception as e:
        bot.send(str(e))
        return {'error': str(e)}


@app.get("/holders/{contract}")
def holders(contract: str, addresses: bool = True, contracts: bool = True, limit: int = 0):
    start = timer()

    try:
        logger.debug(f'API ENTRY: holders({contract}, {addresses}, {contracts}, {limit})')
        result = db.execute_raw(sql.select_holders(contract, addresses, contracts, limit))
        logger.debug(f'API RESULT after {timer() - start:.4f} seconds: {result}')

        return result

    except Exception as e:
        bot.send(str(e))
        return {'error': str(e)}


@app.get("/balance/{address}")
def balance(address: str, contract: str = None):
    start = timer()

    logger.debug(f'API ENTRY: balance({address}, {contract})')

    try:
        if contract:
            result = db.execute_raw(sql.select_balance(address, contract))
            logger.debug(f'API RESULT after: {timer() - start:.4f} seconds: {result}')

            if result and result[0] and result[0][0]:
                return float(result[0][0])
            else:
                return 0

        else:
            result = db.execute_raw(sql.select_balances(address))
            logger.debug(f'API RESULT after: {timer() - start:.4f} seconds: {result}')

            return result

    except Exception as e:
        bot.send(str(e))
        return {'error': str(e)}


@app.get("/state")
def state(contract: str = None):
    logger.debug(f'API ENTRY: state({contract})')

    if contract:
        result = db.execute(sql.select_contract(contract))
        logger.debug(f'API RESULT: {result}')
        # TODO

    else:
        result = db.execute(sql.select_state())  # TODO
        logger.debug(f'API RESULT: {result}')


@app.get("/contract/{contract}")
def contract(contract: str):
    logger.debug(f'API ENTRY: contract({contract})')
    result = db.execute('contract_select', {'c': f'{contract}'})
    logger.debug(f'API RESULT: {result}')
    return result[0][0]


@app.get("/contracts")
def contracts(contract: str = None, lst001: bool = False, lst002: bool = False, lst003: bool = False):
    logger.debug(f'API ENTRY: contracts({contract}, {lst001}, {lst002}, {lst003})')
    result = db.execute(sql.select_contracts(contract, lst001, lst002, lst003))
    logger.debug(f'API RESULT: {result}')
    return result


uvicorn.run(app, host=cfg.get('host'), port=cfg.get('port'))
