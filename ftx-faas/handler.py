from decimal import Decimal
from datetime import datetime, timedelta

from cryptofeed import FeedHandler
from cryptofeed.callback import BookCallback, TradeCallback
from cryptofeed.defines import TRADES, BID, ASK, L2_BOOK, TICKER

import cryptofeed.exchange as ce
FTX = ce.ftx.FTX

# Need class to have a variable that can be modified while doing async stuff
class CallBack:
    def __init__(self, timeframe):
        self.trades = [] # [{feed, symbol, order_id, timestamp, side, amount, price, receipt_timestamp}]
        self.timeframe = 60
        self.trades_per_timeframe = 0
        self.same_size_orders = {} # key is order price: {data, is, here}
        self.callback_url = None
        
    async def trade(self, feed, symbol, order_id, timestamp, side, amount, price, receipt_timestamp):
        # Check data is the right types aka no error
        tests_passed = isinstance(timestamp, float) and isinstance(side, str) and \
                       isinstance(amount, Decimal)  and isinstance(price, Decimal)
                       
        if amount > 0.00000001 and tests_passed:
            # add trade to trade list
            self.trades.append({'feed': feed, 'symbol': symbol, 
                                'order_id': order_id, 'timestamp': timestamp, 
                                'side': side, 'amount': amount, 'price': price, 
                                'receipt_timestamp': receipt_timestamp})

            await self.drop_trades_older_than_timeframe()
            self.trades_per_timeframe = self.trades.__len__() # when tf did it change to __len__ >> cuz async?
            
            #POSTGRES Option
            if self.callback_url:
                return f'Trades Per Minute: {self.trades_per_timeframe}'
            else:
                try:
                    import http.client
                    import json
                    conn = http.client.HTTPSConnection('endln48up6sz2zh.m.pipedream.net')
                    conn.request(
                        "POST", "/", 
                        json.dumps({"Trades Per Minute:": self.trades_per_timeframe}), 
                        {'Content-Type': 'application/json'}
                    ) #subprocess.call(['curl', '-i', '-d', f'Trades Per Minute: {self.trades_per_timeframe}', callback_url])
                except Exception as e:
                    return 'Exception POSTing to callback url: '+str(e)
            #print(f"Timestamp: {timestamp} Cryptofeed Receipt: {receipt_timestamp} Feed: {feed} Pair: {symbol} ID: {order_id} Side: {side} Amount: {amount} Price: {price}") 
        
    async def drop_trades_older_than_timeframe(self):
        oldest_timestamp_allowed = (datetime.now() - timedelta(seconds=self.timeframe)).timestamp()
        self.trades[:] = [trade for trade in self.trades if trade.get('timestamp') > oldest_timestamp_allowed]


def handle(req):#: Union[dict, str]):
    
    # if isinstance(req, dict):
    #     exchange='FTX', 
    #     symbols: Union[list=['BTC-PERP'], 
    #     callback_url: str, 
    #     postgres_conn_args: dict

    CB = CallBack(timeframe=60)
    f = FeedHandler()
    f.add_feed(
        FTX(symbols=['BTC-PERP'], 
            channels=[TRADES],
            callbacks={TRADES: TradeCallback(CB.trade)}))
    f.run()
    return "Started FTX: BTC-PERP 'TRADES Per Minute' feed at callback URL endln48up6sz2zh.m.pipedream.net"# (default: 127.0.0.1:8888)"




