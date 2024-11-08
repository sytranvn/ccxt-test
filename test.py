"""
Testing CCXT Binance future stream.
"""
import asyncio
import os
import time
from typing import List
import ccxt.pro as ccxt
from pprint import pp

async def say_after(delay, what):
    await asyncio.sleep(delay)
    print(what)

exchange = ccxt.binance({"apiKey": os.environ["APIKEY"],
                         "secret": os.environ["SECRET"],
                         "options": {"defaultType": "future"}})

async def watch_price():
    pp("watch price task started")
    tick_count = 0
    while True:
        await exchange.watch_bids_asks()
        await asyncio.sleep(0)
        tick_count +=1


async def watch_positions():
    pp("watch positions task started")
    position_count = 0
    while True:
        positions = await exchange.watch_positions()
        for p in positions:
            pp(dict(id=p["id"],
                    side=p["side"],
                    symbol=p["symbol"],
                    leverage=p["leverage"],
                    entryPrice=p["entryPrice"],
                    contractSize=p["contractSize"],
                    contracts=p["contracts"]))
        await asyncio.sleep(0)
        position_count += 1

async def watch_orders():
    pp("watch orders task started")
    order_count = 0
    while True:
        orders = await exchange.watch_orders(params={"type": "future"})
        for p in orders:
            pp(dict(id=p["id"],
                    market="future",
                    side=p["side"],
                    symbol=p["symbol"],
                    amount=p["amount"],
                    remaining=p["remaining"],
                    reduceOnly=p["reduceOnly"]))
            order_count += 1
        orders = await exchange.watch_orders(params={"type": "spot"})
        for p in orders:
            pp(dict(id=p["id"],
                    market="spot",
                    side=p["side"],
                    symbol=p["symbol"],
                    amount=p["amount"],
                    remaining=p["remaining"],
                    reduceOnly=p["reduceOnly"]))
            order_count += 1
        await asyncio.sleep(0)

async def watch_trades():
    pp("watch trades task started")
    order_count = 0
    while True:
        orders = await exchange.watch_my_trades()
        for p in orders:
            pp(dict(e="trade",
                    id=p["id"],
                    side=p["side"],
                    symbol=p["symbol"],
                    amount=p["amount"],
                    ))
        await asyncio.sleep(0)
        order_count += 1

async def watch_balances():
    pp("watch balances task started")
    order_count = 0
    while True:
        balance = await exchange.watch_balance(params={"type": "future"})
        if exchange.balance:
            pp(exchange.balance.keys())
        pp(dict(e="balance",
                free=balance["free"],
                used=balance["used"],
                total=balance["total"],
                debt=balance.get("debt")
                ))
        await asyncio.sleep(0)
        order_count += 1

async def main():

    tasks : List[asyncio.Task] = []
    
    try:
        pp(f"started at {time.strftime('%X')}")
        await say_after(0, 'hello')

        tasks.append(asyncio.create_task(watch_balances()))
        tasks.append(asyncio.create_task(watch_price()))
        tasks.append(asyncio.create_task(watch_positions()))
        tasks.append(asyncio.create_task(watch_orders()))
        tasks.append(asyncio.create_task(watch_trades()))
        
        await asyncio.gather(*tasks)

    except (KeyboardInterrupt, asyncio.CancelledError):
        pp("cancelling")
    finally:
        for t in tasks:
            if not t.cancelled() and not t.done():
                t.cancel()
        await exchange.close()
        pp(f"finished at {time.strftime('%X')}")

asyncio.run(main())
