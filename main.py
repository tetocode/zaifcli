import contextlib
import copy
import logging
import pathlib
import sys
import time
from datetime import datetime
from pprint import pprint
from queue import Queue
from typing import Union, List, Tuple

import gevent.monkey
import gevent.pool
import yaml
from docopt import docopt
from prettytable import PrettyTable
from requests.exceptions import ProxyError
from zaifapi import ZaifTradeApi, ZaifLeverageTradeApi
from zaifapi.api_error import ZaifApiError

gevent.monkey.patch_all()


def gen_float(expression: Union[str, int]):
    if not expression:
        while True:
            yield None
    expression = str(expression)
    n = sys.maxsize
    if ',' in expression:
        expression, n = expression.split(',')
        n = int(n)
    if ':' in expression:
        start, stop, step = expression.split(':')
        start = float(start)
        stop = float(stop or 'inf')
        step = float(step or 0)
    else:
        start = float(expression)
        stop = float('inf')
        step = 0
        n = 1
    asc = True
    if start > stop:
        asc = False
    for i in range(n):
        price = start + step * i
        if asc and stop <= price:
            break
        if not asc and price <= stop:
            break
        yield price


def parse_indexes(s: str):
    indexes = []
    for s in s.split(','):
        if '-' in s:
            start, end = list(map(int, s.split('-')))
            for i in range(start, end + 1):
                indexes.append(i)
        else:
            indexes.append(int(s))
    return indexes


def round_price(currency_pair: str, price: float):
    assert currency_pair, currency_pair
    if currency_pair == 'btc_jpy':
        return int(price)
    return int(price * 1e4) / 1e4


class CredentialPool:
    def __init__(self, credential_path: str, n: int):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.credential_path = str(pathlib.Path(credential_path).absolute())
        self.credentials = []
        self.load_credentials()
        self.q = Queue()
        for key, secret in self.credentials[:n]:
            self.q.put((key, secret))

    def load_credentials(self):
        try:
            with open(self.credential_path) as f:
                self.credentials = yaml.load(f)
        except FileNotFoundError:
            self.logger.critical('credential file not found {}'.format(self.credential_path))
            raise

    @contextlib.contextmanager
    def get_credential(self) -> Tuple[str, str]:
        credential = None
        try:
            credential = self.q.get()
            yield credential
        finally:
            if credential:
                self.q.put(credential)


def main():
    args = docopt("""
    Usage:
        {f} [options] (spot | leverage) list
        {f} [options] (spot | leverage) METHOD [PARAM...]
        {f} [options] board PARAM...

    Options:
      --cancel INDEXES
      --update INDEXES
      --price PRICE  start:stop:step/float/int
      --limit PRICE  start:stop:step/float/int
      --stop PRICE  start:stop:step/float/int
      --interval SECONDS  [default: 0.3]
      --credential PATH  [default: {path}]
      --credential-n N  [default: 100]
      --retry_out_of_inventory
      --serial

    """.format(f=sys.argv[0], path=pathlib.Path.home() / '.zaifbot.yaml'))
    pprint(args)

    trade_api = ZaifTradeApi  # (api_key, api_secret)
    leverage_api = ZaifLeverageTradeApi  # (api_key, api_secret)
    interval = float(args['--interval'])

    credential_pool = CredentialPool(args['--credential'], int(args['--credential-n']))
    serial = args['--serial']
    retry_out_of_inventory = args['--retry_out_of_inventory']

    def retry(api, method_name, *_args, **_kwargs):
        try_n = 0
        with credential_pool.get_credential() as credential:
            fn = getattr(api(*credential), method_name)
            print('#', fn.__name__, _args, _kwargs)
            start = time.time()
            while True:
                try_n += 1
                seconds = interval
                try:
                    _res = fn(*_args, **_kwargs)
                    print('#done try_n={} elapsed={}'.format(try_n, time.time() - start))
                    return _res
                except ZaifApiError as e:
                    s = str(e)
                    seconds = interval
                    if 'time wait restriction, please try later.' in s:
                        seconds = interval * 10
                        logging.error(s)
                    elif 'Sorry, inventory is out of order' in s:
                        if retry_out_of_inventory:
                            seconds = interval * 10
                            logging.warning(s)
                        else:
                            raise
                    elif 'insufficient funds' in s:
                        raise
                    elif 'Insufficient margin' in s:
                        raise
                    elif 'order not found' in s:
                        raise
                    elif 'order already closed' in s:
                        raise
                    elif 'already ordered' in s:
                        raise
                    elif 'max position count is' in s:
                        raise
                    elif 'position still remaining' in s:
                        raise
                    else:
                        # 'trade temporarily unavailable.', etc
                        logging.error(s)
                except ProxyError:
                    pass
                time.sleep(seconds)

    api = trade_api if args['spot'] else leverage_api
    method = args['METHOD']
    params = {}
    for param in args['PARAM']:
        k, v = param.split('=')
        if v.isdigit():
            v = int(v)
        else:
            try:
                v = float(v)
            except ValueError:
                pass
        params[k] = v

    if args['list']:
        import inspect
        for k in sorted(api.__dict__):
            v = api.__dict__[k]
            if k.startswith('_'):
                continue
            if callable(v):
                print(k, inspect.signature(v))
        return

    if args['board']:
        return

    if api == trade_api:
        get_method = 'active_orders'
        cancel_method = 'cancel_order'
        update_method = None
        id_key = 'order_id'
    else:
        assert api == leverage_api
        get_method = 'active_positions'
        cancel_method = 'cancel_position'
        update_method = 'change_position'
        id_key = 'leverage_id'

    if args['--cancel']:
        cancel_indexes = parse_indexes(args['--cancel'])
    else:
        cancel_indexes = []
    if args['--update']:
        assert api == leverage_api
        update_indexes = parse_indexes(args['--update'])
    else:
        update_indexes = []

    price_gen = gen_float(args['--price'])
    limit_gen = gen_float(args['--limit'])
    stop_gen = gen_float(args['--stop'])

    if method == get_method:
        items = print_items(retry(api, method, **params))
        g_list = []
        for i, x in enumerate(items, 1):
            if i in cancel_indexes:
                _id = x['id']
                print('#cancel #{} id={}'.format(x['#'], _id))
                cancel_params = copy.deepcopy(params)
                if args['leverage'] and 'currency_pair' in cancel_params:
                    del cancel_params['currency_pair']
                cancel_params.update({id_key: _id})
                g_list.append(gevent.spawn(retry, api, cancel_method, **cancel_params))
            elif i in update_indexes:
                try:
                    price = next(price_gen)
                    limit = next(limit_gen)
                    stop = next(stop_gen)
                except StopIteration as e:
                    logging.exception(str(e))
                    break
                assert args['leverage']
                _id = x['id']
                update_params = copy.deepcopy(params)
                if args['leverage'] and 'currency_pair' in update_params:
                    del update_params['currency_pair']
                update_params.update({id_key: _id})
                currency_pair = x['currency_pair']
                if x.get('amount_done') == x['amount']:
                    price = None
                price and update_params.update(price=round_price(currency_pair, price))
                limit and update_params.update(limit=round_price(currency_pair, limit))
                stop and update_params.update(stop=round_price(currency_pair, stop))
                print('#update #{} id={} update_params={}'.format(x['#'], _id, update_params))
                g_list.append(gevent.spawn(retry, api, update_method, **update_params))
            if serial:
                gevent.joinall(g_list)
                g_list = []
        if not serial:
            gevent.joinall(g_list)
        if cancel_indexes or update_indexes:
            print_items(retry(api, method, **params))
        return

    price_gen = gen_float(params.get('price', []))

    if params.get('price'):
        g_list = []

        def retry_print(*_args, **_kwargs):
            pprint(retry(*_args, **_kwargs))

        for i, price in enumerate(price_gen, 1):
            print('#{}'.format(i))
            price = round_price(params.get('currency_pair', 'btc_jpy'), price)
            params = params.copy()
            params.update(price=price)
            g_list.append(gevent.spawn(retry_print, api, method, **params))
            if serial:
                gevent.joinall(g_list)
                g_list = []
        if not serial:
            gevent.joinall(g_list)
        return

    res = retry(api, method, **params)
    pprint(res)


def print_items(res) -> List[dict]:
    res = copy.deepcopy(res)
    keys = set()
    for _ in res.values():
        keys.update(_.keys())
    if 'term_end' in keys:
        keys.remove('term_end')
    header = ['#', 'id'] + list(sorted(keys))
    table = PrettyTable(header)
    items = []
    for i, _id in enumerate(sorted(res), 1):
        v = res[_id]
        v['#'] = i
        v['id'] = int(_id)
        row = []
        items.append(v)
        for k in header:
            _ = v.setdefault(k, '')
            if k == 'timestamp' and _:
                _ = datetime.utcfromtimestamp(float(_))
            row.append(_)
        table.add_row(row)
    print(table)
    return items


if __name__ == '__main__':
    main()
