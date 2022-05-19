import os
import sys
import uvicorn

from config import Config
from database import DB
from loguru import logger
from typing import Union
from fastapi import FastAPI
from datetime import timedelta
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

# TODO: API for total stamps used for address
# TODO: API for 'network involvement' of an address - how much other addresses transacted with address?
# TODO: Use similar API as default Block Service
# TODO: Add API for 'TAU forever lost'
# TODO: Add API for address Toplist (TAU or any other token)
# TODO: API to subscribe to state changes
# TODO: API for which contract holds which funds

app = FastAPI(title='BlockJuggler API')

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True
)

cfg = Config(os.path.join('cfg', 'config.json'))
db = DB(cfg)

logger.remove()

logger.add(
    sys.stderr,
    level=cfg.get('log_level'))

logger.add(
    os.path.join('log', 'api_{time}.log'),
    retention=timedelta(days=cfg.get('log_retention')),
    format='{time} {name} {message}',
    level=cfg.get('log_level'),
    rotation='10 MB',
    diagnose=True)


@app.get("/", response_class=HTMLResponse)
def read_root():
    return """
    <html>
    <head>
        <style>
        div {
            width: 320px;
            height: 320px;
            --lg: linear-gradient(135deg, #fff 50%, transparent 0);
            background:
                var(--lg) 0 0 / 100% 100%,
                var(--lg) 0 0 / 50% 50%,
                var(--lg) 0 0 / 25% 25%,
                var(--lg) 0 0 / 12.5% 12.5%,
                var(--lg) 0 0 / 6.25% 6.25%,
                var(--lg) 0 0 / 3.125% 3.125%;
            background-color: #000;
            mask: linear-gradient(-45deg, #000 50%, transparent 0);
            transform: skew(27deg) translateX(-25%);
        }

        html,
        body {
            height: 100%;
            margin: 0;
            display: grid;
            place-items: center;
        }

        div {
            will-change: transform;
        }
        </style>
    </head>
    <body>
        <div></div>
    </body>
    </html>
    """


@app.get("/balances/{address}")
def balances(address: str):
    balances = db.execute('balances_select')

    for balance in balances:
        contract = balance[0].replace('.balances:ae7d14d6d9b8443f881ba6244727b69b681010e782d4fe482dbfb0b6aca02d5d', '')

        amount = balance[1]
        if type(amount) is dict and len(amount) == 1 and '__fixed__' in amount:
            amount = amount['__fixed__']

        if float(amount) == 0:
            continue

        print(contract, amount)


@app.get("/state_snapshot")
def balances():
    db.execute()


uvicorn.run(app, host=cfg.get('api_host'), port=cfg.get('api_port'))
