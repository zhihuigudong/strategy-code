
# coding: utf-8

# In[ ]:

# 使用模块导入
import numpy as np
import pandas as pd
import datetime
import math
import time
from datetime import date
import re

'''
银行行业增强策略-银行翻倍策略
'''
# 参数设置：
name = '银行翻倍策略，择时'
# 对比基准设置'801780.SL''000300.SH' '000905.SH'
num1 = '801780.SL'
# 全仓(1)或者半仓(0.5)
num4 = 1
# 选股条件
num2 = '银行股 上市天数>200'
# 目标选股数量
num3 = 3
#长期空仓天数
num5 = 250
#市场空仓对应的PB分位数阈值
num6 = 0.8

def init(context):
    set_benchmark(num1)
    get_iwencai(num2)
    context.signal = True  
    context.b = True 
    g.output_stocklist = {}
    g.day = 0
    g.step = 5
    
def before_trading(context):
    #完成选股
    if g.day/g.step==0:
        g.stock = GrahamStockFilter(context)
        for s in g.stock:
            g.output_stocklist[s] = num4 / len(g.stock)
        set_stock_picks(g.output_stocklist)
        g.output_stocklist.clear()

def handle_bar(context,bar_dict):
    if g.day%g.step==0:
        patt=r'择时'
        pattern = re.compile(patt)
        result = pattern.findall(name)
        if result:
            period, context.signal = time_select(context)
            if context.signal == 'kongcang' and context.b is True:
                context.b = False
                context.date = get_datetime() + datetime.timedelta(days=period)
                for stock in list(context.portfolio.stock_account.positions.keys()):
                    order_target_value(stock, 0)
                log.info('空仓信号在' + get_datetime().strftime('%Y-%m-%d') + '触发，空仓' + str(period) + '天')
                    
            if context.b is False and get_datetime() > context.date:
                context.b = True
                
            if context.signal == 'chaodi':
                context.b = True
                context.signal = True
                    
                    
            if context.signal is True and context.b is True:  
                #buylist = GrahamStockFilter(context)  
                for hold in list(context.portfolio.stock_account.positions.keys()):
                    if hold not in list(g.stock):
                        order_target_value(hold, 0)
                for stock in g.stock:
                        order_target_percent(stock, num4 / len(g.stock))
        else:
            #buylist = GrahamStockFilter(context)
            for hold in list(context.portfolio.stock_account.positions):
                if hold not in list(g.stock):
                    order_target_value(hold, 0)
            for stock in g.stock:
                order_target_percent(stock, num4 / len(g.stock))

def after_trading(context):
    g.day += 1

def GrahamStockFilter(context,overflow=0):
    df1 = Data(context)
    buylist = Yhfb(context,df1)
    log.info(get_datetime().strftime('%Y-%m-%d') + '选股为:' + str(buylist)[:])
    return buylist
    
# 数据获取
def Data(context):
    yesterday = get_last_datetime().strftime('%Y%m%d')
    today = get_datetime().strftime('%Y%m%d')
    stock_list = context.iwencai_securities
    #stock_list = get_industry_stocks('T1901')
    value = get_price(stock_list, None, today, '1d', ['is_paused', 'is_st','open','high_limit','low_limit'], True, None, 1, is_panel=1)
    stock_list = [stock for stock in stock_list if value['is_paused'].ix[0,stock] == 0]
    stock_list = [stock for stock in stock_list if value['is_st'].ix[0,stock] == 0]
    stock_list = [stock for stock in stock_list if value['open'].ix[0,stock] != value['high_limit'].ix[0,stock] and value['open'].ix[0,stock]!= value['low_limit'].ix[0,stock]]
    log.info('股票池数量：%d' %len(stock_list))
    #获取财务数据
    q = query(valuation.symbol, income.basic_eps, valuation.pb,valuation.pe, valuation.market_cap,
              income_one_season.profit_from_operations, valuation.capitalization, profit_one_season.roe,balance.total_liabilities,
              growth_one_season.opt_profit_growth_ratio).filter(valuation.symbol.in_(stock_list)).order_by(
        valuation.symbol)

    df1 = get_fundamentals(q, date=yesterday)
    df1 = df1.set_index(df1['valuation_symbol'])
    df1 = df1[df1['profit_one_season_roe'] > 0]
    df1['close'] = list([float("nan")] * (len(df1.index)))
    true_list = list(df1.index)
    close_p = history(true_list, ['close','turnover'], 1, '1d', skip_paused=False, fq='pre', is_panel=1)
    for stock in true_list:
        df1.loc[stock, 'close'] = close_p['close'][stock].values[0]
        df1.loc[stock, 'volume'] = close_p['turnover'][stock].values[0]/10000
    return df1

def Yhfb(context,df1):
    df1 = df1[df1['growth_one_season_opt_profit_growth_ratio']>0]
    #pb小于3的股票
    df1 = df1[df1['valuation_pb']<3]
    #成交额筛选
    df1 = df1[df1['volume']>5000]
    
    df1['double_time'] = df1.apply(
        lambda row: round(math.log(2.0 * row['valuation_pb']/(1+row['growth_one_season_opt_profit_growth_ratio']/100), (1.0 + row['profit_one_season_roe'] / 100)), 2), axis=1)
    df_double = df1.sort_values('double_time', ascending=True)
    df1 = df_double[:num3]  
    buylist = list(df1['valuation_symbol'].values)
    return buylist

def time_select(context):
    yesterday = get_last_datetime().strftime('%Y-%m-%d')
    qt = query(valuation.symbol, valuation.pb).filter()
    df = get_fundamentals(qt, date=yesterday)
    # 计算昨天市场所有股票PB值的分位数
    factor_quantiles = df.dropna().quantile([num6])
    PB = factor_quantiles.iloc[0].values 
    '''
    # 计算昨天市场上跌停的股票占比
    stock_list = list(get_all_securities('stock', yesterday).index)
    pct_all = history(stock_list, ['quote_rate'], 5, '1d',
                  skip_paused=False, fq=None, is_panel=1)
    pct_list = []
    for i in range(5):
        values = list(pct_all.iloc[0,i,])
        pct = (len([x for x in values if x <= -9.5])) / len(stock_list)
        pct_list.append(pct)
    pct = max(pct_list)
    log.info('上周最大跌停股票占比: %.2f%%' %(pct*100))
    '''
    # 择时空仓条件设置
    if PB >= 10:
        log.info('空仓',PB)
        return num5, 'kongcang'
    #elif pct > 0.1:
        #return num6, False
    #elif PB <= 5:
        #return 0, 'chaodi'
    else:
        return 0, True

