import pandas as pd
import datetime
import os
import pathlib

import xlwings as xw

import sys
sys.path.append(str(pathlib.Path(__file__).parent.parent.absolute()))

import db_benchmarks

db = db_benchmarks.BMDatabase()


@xw.func
@xw.ret(index=False)
def get_index_data(start_date, end_date, index_id):
    start_date = str(start_date)[:10]
    end_date = str(end_date)[:10]
    df = db.get_index_data(index_id, start_date, end_date)
    return df

@xw.func
def get_index_close(end_date, index_id, last=False):
    end_date = str(end_date)[:10]
    df = db.get_index_data(index_id, end_date=end_date, last=last)
    if df.empty:
        return "#N/A"
    else:
        return df.iloc[0][2]

@xw.func
def get_index_stddev(start_date, end_date, index_id=None):
    ret = db.get_index_stddev(start_date, end_date, index_id)
    return ret
 
@xw.func
def get_scrip_return(start_date, end_date, symbol):
    start_date = str(start_date)[:10]
    end_date = str(end_date)[:10]
    ret = db.get_scrip_return(start_date, end_date, symbol)
    return ret

@xw.func
def get_scrip_stddev(symbol, start_date, end_date):
    ret = db.get_scrip_stddev(symbol, start_date, end_date)
    return ret

@xw.func
def get_scrip_correl(symbol1, symbol2, start_date, end_date):
    ret = db.get_scrip_correl(symbol1, symbol2, start_date, end_date)
    return ret

@xw.func
def get_scrip_avg_volume(symbol, start_date, end_date):
    ret = db.get_scrip_avg_volume(symbol, start_date, end_date)
    return ret

@xw.func
def get_scrip_shares(symbol):
    ret = db.get_scrip_info(symbol)
    return ret[3]

@xw.func
def get_scrip_info(symbol, info_id):
    ret = db.get_scrip_info(symbol)
    return ret[info_id]

@xw.func
def get_scrip_sector(symbol):
    ret = db.get_scrip_info(symbol)
    sec_id = ret[2]
    qry = f"SELECT sector_name FROM psx_sectors WHERE sector_id='{sec_id}'"
    ret2 = db.conn.execute(qry)
    return ret2.fetchone()

@xw.func
def get_scrip_close(symbol, start_date=None, end_date=None, last=False):
    if last:
        df = db.get_scrip_data(symbol, end_date=end_date, last=last)
        return df['close'].iloc[0]
    else:
        return "asd"
    
@xw.func
def get_scrip_traded_days(symbol, start_date, end_date):
    df = db.get_scrip_data(symbol, start_date, end_date)
    return df.shape[0]

@xw.func
def get_index_traded_days(start_date, end_date, index_id):
    df = db.get_index_data(index_id, start_date, end_date)
    return df.shape[0]

@xw.func
def get_index_correl(start_date, end_date, index_id, symbol):
    ret = db.get_index_correl(start_date, end_date, index_id, symbol)
    return ret