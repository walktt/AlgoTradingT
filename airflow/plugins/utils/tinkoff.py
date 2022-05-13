import tinkoff.invest as ti
import pandas as pd
from configparser import ConfigParser
import datetime

def _get_api_params_from_config() -> dict:
    config_parser = ConfigParser()
    config_parser.read('/usr/local/airflow/tinkoff.cfg')

    return {
        'token': config_parser.get('core', 'TOKEN_TINKOFF')
        # 'USE_SANDBOX' : config_parser.get('core','USE_SANDBOX')
    }

def get_figi_from_ticker(name, client):
    instruments = client.instruments.futures()
    for instrument in instruments.instruments:
        if instrument.ticker == name:
            break
    return instrument.figi

def calcDirection(open, close):
    if open==close:
        return '='
    if close-open>0:
        return '+'
    if close-open<0:
        return '-'
    else:
        return 'err'

def create_df_from_ti(candles: [ti.HistoricCandle]):
    df = pd.DataFrame ([{
        'time' : c.time+datetime.timedelta(hours=3),
        'open' : c.open.units+c.open.nano/1000000000,
        'close' : c.close.units+c.close.nano/1000000000,
        'low' : c.low.units+c.low.nano/1000000000,
        'high' : c.high.units+c.high.nano/1000000000,
        'dir' : calcDirection(c.open.units+c.open.nano/1000000000,c.close.units+c.close.nano/1000000000)
    } for c in candles])
    return df

def get_data_by_ticker_and_period(ticker:str, period_in_days: int = 300, freq = ti.CandleInterval.CANDLE_INTERVAL_DAY) -> pd.DataFrame:
    # t.Dv3YZa7IiD5aFZQZS2rXpxVt9K6AqeBVmEF1LpV6_YKNw5t2gbxfFkPzU7KbEeQJlyJyTgVjkZZEvCHpR - NrnQ
    today_date = datetime.date.today()
    today_date_start = datetime.datetime(today_date.year, today_date.month, today_date.day)
    # client = tinvest.SyncClient(**_get_api_params_from_config())

    with ti.Client(**_get_api_params_from_config()) as client:
        figi = get_figi_from_ticker(ticker, client)
        raw_data = client.market_data.get_candles(
                figi=figi,
                from_=today_date_start - datetime.timedelta(days=period_in_days),
                # to=datetime.datetime.now(),
                to = today_date_start,
                interval=freq
            )

    return create_df_from_ti(raw_data.candles)

def get_position_by_ticker(ticker:str, client):
    
