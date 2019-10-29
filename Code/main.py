# -*- coding: utf-8 -*-
"""
Created on Sat Oct 12 18:20:16 2019

@author: jiaxu
"""

import database as db
import getData as gd
import pandas as pd
import numpy as np
import pickle
import backtest as bt

from sqlalchemy import create_engine
from sqlalchemy import MetaData

location_of_pairs = '../Data/pair.csv'

engine = create_engine('sqlite:///pairTradingjx.db')
conn = engine.connect()
conn.execute("PRAGMA foreign_keys = ON")

# MetaData is a container object that keeps together many different features of a database 
metadata = MetaData()
metadata.reflect(bind=engine)

pd.set_option('display.max_columns', 10)

def main():
    
    #get data
    sp500=gd.getIndex()
    #sp500=pd.read_csv('sp500.csv')
    db.createSP500indexTable(metadata,engine,table_name='SP500')
    sp500.to_sql('SP500',con=engine, if_exists='replace', index=False)
    print("sp500 in")

    db.createStockpairsTable('Pairs', metadata, engine)
    pairs = pd.read_csv(location_of_pairs)
    pairs["TrueRange"] = 0.0
    pairs["ProfitLoss"] = 0.0
    pairs.to_sql('Pairs', con=engine, if_exists='replace', index=False)
    print("pairs in")
    
    db.createTradepairsTable("TradePairs",metadata,engine)
    
    
    #AggMinprice=gd.getMinData()
    with open ('../Data/clean_min_data.p', 'rb') as fp:
        AggMinprice = pickle.load(fp) 
        
    tables = ['AggMinprice','Pair1StocksMin', 'Pair2StocksMin']
    
    for table in tables:
        db.createStockmpriceTable(table, metadata, engine)
    
    for table in tables:
        db.clearTable(table, metadata, engine)
    AggMinprice.to_sql('AggMinprice',con=engine, if_exists='replace', index=False)
    
    backtestday=AggMinprice['Date'].unique()
    
    db.selectSQLstatement('Pair1StocksMin',engine)
    db.selectSQLstatement('Pair2StocksMin',engine)
    
    #day_data=gd.getAllDailyData(sp500)
#    with open ('day_data.p','wb') as fp:
#        pickle.dump(day_data, fp)
    with open('../Data/day_data.p','rb') as fp:
        day_data=pickle.load(fp)
    db.createSP500daypriceTable(metadata,engine,table_name='SP500dprice')
    day_data.to_sql('SP500dprice',con=engine, if_exists='replace', index=False)
    print("sp500dp in")
    
    tables = ['Pair1StocksDay', 'Pair2StocksDay']
    
    for table in tables:
        db.createStockdpriceTable(table, metadata, engine)
    
    
    for table in tables:
        db.clearTable(table, metadata, engine)
        
    db.selectSQLstatement('Pair1StocksDay',engine)
    db.selectSQLstatement('Pair2StocksDay',engine)
    
#    engine.execute('Drop Table if exists PairmPrices;')
#    db.createPairmpricesTable('PairmPrices',metadata,engine)

    engine.execute('Drop Table if exists PairdPrices;')
    db.createPairdpricesTable('PairdPrices', metadata, engine)
    
    db.selectSQLstatement('PairdPrices',engine)
    select_st = "Select * from PairdPrices;"
    result_set = engine.execute(select_st)
    result_df = pd.DataFrame(result_set.fetchall())
    result_df.columns = result_set.keys()
    
    result_df['TrueRange']=abs(np.log(result_df['Close1']/result_df['Close2'])-np.log(result_df['Open1']/result_df['Open2']))
    result_df_tr = result_df.groupby(['Ticker1', 'Ticker2'])['TrueRange'].quantile(q=0.98)
    
    result_df_tr=result_df_tr.reset_index()
    result_df_tr['TrueRange']=np.exp(result_df_tr['TrueRange'])
    
    result_df_tr.to_sql('tmp', con=engine, if_exists='replace')
    
    update_st = """
    Update Pairs set TrueRange = (SELECT tmp.TrueRange
                                        FROM tmp 
                                        WHERE tmp.Ticker1 = Pairs.Ticker1
                                        and tmp.Ticker2 = Pairs.Ticker2);
    """
    
    db.executeSQLstatement(update_st, engine)
    
    droptable_st = 'Drop Table if exists tmp'
    db.executeSQLstatement(droptable_st, engine)
    

    db.createTradesTable('Trades',metadata,engine)
    
    btsum=bt.backtest_func(backtestday,metadata,engine,interval=30)
    return backtestday,btsum
    
backtestday,btsum=main()
