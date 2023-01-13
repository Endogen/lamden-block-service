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
# TODO: Total rewards for address
# TODO: Use Starlite instead of FastAPI? https://github.com/starlite-api/starlite

app = FastAPI(title='LAPI')

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True
)

cfg = Config('cfg', 'api.json')

tg_cfg = Config('cfg', 'tgbot.json')
bot = TelegramBot(tg_cfg)

db_cfg = Config('cfg', 'db.json')
db = DB(db_cfg)

logger.remove()

logger.add(
    sys.stderr,
    level=cfg.get('log_level'))

logger.add(
    Path('log', 'api_{time}.log'),
    retention=timedelta(days=cfg.get('log_retention')),
    format='{time} {level} {name} {message}',
    level=cfg.get('log_level'),
    rotation='10 MB',
    diagnose=True)


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

        logger.debug(f'API --> db_size()')
        result = db.execute(sql.select_db_size(), {'n': db_cfg.get('db_name')})
        logger.debug(f'API <-- after {timer() - start:.3f} seconds')

        return result[0][0]

    except Exception as e:
        bot.send(repr(e))
        return {'error': repr(e)}


@app.get("/holders/{contract}")
def get_holders(contract: str, addresses: bool = True, contracts: bool = True, limit: int = 0):
    start = timer()

    try:

        logger.debug(f'API --> get_holders({contract}, {addresses}, {contracts}, {limit})')
        result = db.execute(sql.select_holders(contract, addresses, contracts, limit))
        logger.debug(f'API <-- after {timer() - start:.3f} seconds')

        return result

    except Exception as e:
        bot.send(repr(e))
        return {'error': repr(e)}


@app.get("/balance/{address}")
def get_balance(address: str, contract: str = None):
    start = timer()

    try:

        logger.debug(f'API --> get_balance({address}, {contract})')

        if contract:
            result = db.execute(sql.select_balance(address, contract))
            logger.debug(f'API <-- after {timer() - start:.3f} seconds')

            if result and result[0] and result[0][0]:
                return float(result[0][0])
            else:
                return 0

        else:
            result = db.execute(sql.select_balances(address))
            logger.debug(f'API <-- after {timer() - start:.3f} seconds')

            return result

    except Exception as e:
        bot.send(repr(e))
        return {'error': repr(e)}


@app.get("/raw_state/{key}")
def get_raw_state(key: str = None):
    start = timer()

    try:

        logger.debug(f'API --> get_raw_state({key})')
        result = db.execute(sql.select_raw_state(), {'k': key})
        logger.debug(f'API <-- after {timer() - start:.3f} seconds')

        return result[0][0]

    except Exception as e:
        bot.send(repr(e))
        return {'error': repr(e)}


@app.get("/raw_states/{key}")
def get_raw_states(key: str = None):
    start = timer()

    try:

        logger.debug(f'API --> get_raw_states({key})')
        result = db.execute(sql.select_raw_states(key))
        logger.debug(f'API <-- after {timer() - start:.3f} seconds')

        return result

    except Exception as e:
        bot.send(repr(e))
        return {'error': repr(e)}


@app.get("/state/{key}")
def get_state(key: str = None):
    start = timer()

    try:

        logger.debug(f'API --> get_state({key})')
        result = db.execute(sql.select_state(clean=True), {'k': key})
        logger.debug(f'API <-- after {timer() - start:.3f} seconds')

        return result[0][0]

    except Exception as e:
        bot.send(repr(e))
        return {'error': repr(e)}


@app.get("/states/{key}")
def get_states(key: str = None):
    start = timer()

    try:

        logger.debug(f'API --> get_states({key})')
        result = db.execute(sql.select_states(key, clean=True))
        logger.debug(f'API <-- after {timer() - start:.3f} seconds')

        return result

    except Exception as e:
        bot.send(repr(e))
        return {'error': repr(e)}


@app.get("/contract/{contract}")
def get_contract(contract: str):
    start = timer()

    try:

        logger.debug(f'API --> get_contract({contract})')
        result = db.execute(sql.select_contract(), {'c': contract})
        logger.debug(f'API <-- after {timer() - start:.3f} seconds')

        return result[0][0]

    except Exception as e:
        bot.send(repr(e))
        return {'error': repr(e)}


# TODO: Make sure it works
@app.get("/contracts")
def get_contracts(name: str = None, lst001: bool = False, lst002: bool = False, lst003: bool = False):
    start = timer()

    try:

        logger.debug(f'API --> get_contracts({name}, {lst001}, {lst002}, {lst003})')
        result = db.execute(sql.select_contracts(), {'n': name})
        logger.debug(f'API <-- after: {timer() - start:.3f} seconds')
        return result

    except Exception as e:
        bot.send(repr(e))
        return {'error': repr(e)}


uvicorn.run(app, host=cfg.get('host'), port=cfg.get('port'))
