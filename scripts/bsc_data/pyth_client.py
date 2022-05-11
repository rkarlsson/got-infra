#!/usr/bin/env python3

import asyncio
from pythclient.pythclient import PythClient
from pythclient.pythaccounts import PythPriceAccount
from pythclient.utils import get_key

use_program=True
solana_network="devnet"
async def print_stuff():
    async with PythClient(
        first_mapping_account_key=get_key(solana_network, "mapping"),
        program_key=get_key(solana_network, "program") if use_program else None,
    ) as c:
        await c.refresh_all_prices()
        products = await c.get_products()
        for p in products:
            print(p.attrs)
            prices = await p.get_prices()
            for _, pr in prices.items():
                print(
                    pr.price_type,
                    pr.aggregate_price_status,
                    pr.aggregate_price,
                    "p/m",
                    pr.aggregate_price_confidence_interval,
                )


loop = asyncio.get_event_loop()
loop.run_until_complete(print_stuff())
loop.close()