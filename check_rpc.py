#!/usr/bin/env python3
"""
One-shot RPC endpoint check. Polls every endpoint (and the public ground truth)
for every chain in the config once, prints heights and lags to stdout, and
exits. Useful for diagnosing which URLs are broken without running the full
monitor daemon.

Usage:
  ./check_rpc.py [-c CONFIG] [chain ...]

Defaults: config is monitor_rpc.toml next to this script; all chains are
checked. Pass chain names (btc eth arb sol dot hub tron) to limit.
"""

import argparse
import asyncio
import sys
from pathlib import Path

import aiohttp

import monitor_rpc
from monitor_rpc import (
    CHAIN_RPC, ENDPOINTS, GROUND_TRUTH,
    fetch_height, fmt_error, load_config,
)


async def check_chain(client, chain):
    """Poll one chain's endpoints + ground truth. Returns {name: int | Exception}."""
    kind = CHAIN_RPC[chain]
    gt_kind, gt_url = GROUND_TRUTH[chain]
    tasks = {name: asyncio.create_task(fetch_height(client, kind, url))
             for name, url in ENDPOINTS[chain].items()}
    tasks['ground_truth'] = asyncio.create_task(fetch_height(client, gt_kind, gt_url))
    out = {}
    for name, task in tasks.items():
        try:
            out[name] = await task
        except Exception as ex:
            out[name] = ex
    return out


def render(chain, results):
    heights = [v for v in results.values() if isinstance(v, int)]
    max_h = max(heights) if heights else None
    print(f'=== {chain} (max {max_h}) ===')
    for name, val in results.items():
        if isinstance(val, Exception):
            print(f'  {name:<14} ERROR  {fmt_error(val)}')
        else:
            lag = (max_h - val) if max_h is not None else 0
            tail = f'  (-{lag})' if lag > 0 else ''
            print(f'  {name:<14} {val}{tail}')


async def amain(config_path, chains):
    load_config(config_path)
    if not ENDPOINTS:
        print(f'Error: no [endpoints.<chain>] sections in {config_path}', file=sys.stderr)
        sys.exit(1)

    chains = chains or list(ENDPOINTS)
    unknown = [c for c in chains if c not in ENDPOINTS]
    if unknown:
        print(f'Unknown or unconfigured chains: {unknown}', file=sys.stderr)
        sys.exit(1)

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as client:
        for chain in chains:
            results = await check_chain(client, chain)
            render(chain, results)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-c', '--config', default=None,
                    help='Path to monitor_rpc.toml (default: next to this script)')
    ap.add_argument('chains', nargs='*', help='Chains to check; default: all configured')
    args = ap.parse_args()

    config_path = Path(args.config) if args.config else Path(__file__).parent / 'monitor_rpc.toml'
    if not config_path.exists():
        print(f'Error: config file not found: {config_path}', file=sys.stderr)
        sys.exit(1)

    asyncio.run(amain(config_path, args.chains))


if __name__ == '__main__':
    main()
