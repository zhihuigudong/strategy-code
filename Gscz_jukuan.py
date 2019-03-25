#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#避免编辑器IDE报错
g,set_benchmark,set_option,log,set_order_cost,OrderCost,run_daily,send_message=0
query,valuation,get_fundamentals,order_target,order_target_value,get_trades,get_trade_days=0
run_monthly,get_iwencai,order_target_value,order_target_percent,get_datetime,get_price,history=0
get_last_datetime,income,income_one_season,growth_one_season,profit_one_season,balance=0;
get_concept_stocks,run_weekly,get_index_stocks,income_sq,profit_sq,growth_sq,get_all_securities=0
get_industry_stocks,indicator=0
from jqdata import *
'''
格氏成长策略
'''
# 参数设置：

#策略名'格氏成长,择时'
name = '格氏成长,择时'
# 对比基准设置
num1 = '000300.XSHG'
# 选股条件
#num2 = '沪深300 上市天数>200'
num2 = '000300.XSHG'
# 目标选股数量
num3 = 20
#市场空仓对应的PB分位数阈值
num4 = 0.8
#长期空仓天数
num5 = 240
#中期空仓天数
num6 = 90

def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')
    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    context.signal = True  
    context.b = True 
    #同花顺行业选取接口 get_iwencai(num2) 智能识别字符串,聚宽版本为get_concept_stocks
    g.security = get_index_stocks(num2)
    run_weekly(handle, weekday=3, time="open",reference_security = '000300.XSHG')
    
def handle(context):
    import datetime,re
    patt=r'择时'
    pattern = re.compile(patt)
    result = pattern.findall(name);print result
    if result:
        #print("entering 择时")
        period, context.signal = time_select(context)
        if context.signal is not True and context.b is True:
            context.b = False
            context.date = context.current_dt + datetime.timedelta(days=period)
            for stock in list(context.portfolio.positions.keys()):
                order_target_value(stock, 0)
            log.info('空仓信号在' + context.current_dt.strftime('%Y-%m-%d') + '触发，空仓' + str(period) + '天')
            
        if context.b is False and context.current_dt > context.date:
            context.b = True  
        
        if context.signal is True and context.b is True:  
            buylist = GrahamStockFilter(context)
            #print "buylist",buylist
            for hold in list(context.portfolio.positions.keys()):
                if hold not in list(buylist):
                    order_target_value(hold, 0)
            for stock in buylist:
                order_target_value(stock, context.portfolio.total_value*0.999/len(buylist))
    else:
        #print("entering 不需要择时")
        buylist = GrahamStockFilter(context)
        for hold in list(context.portfolio.positions):
            if hold not in list(buylist):
                order_target_value(hold, 0)
        for stock in buylist:
            order_target_value(stock, context.portfolio.total_value*0.999/len(buylist))
        

        
def GrahamStockFilter(context,overflow=0):
    df1 = Data(context)
    buylist = Gscz(context,df1)
    log.info(context.current_dt.strftime('%Y-%m-%d') + '选股为:' + str(buylist)[:])
    return buylist
    
# 数据获取
def Data(context):
    yesterday = context.previous_date.strftime('%Y-%m-%d')
    today = context.current_dt.strftime('%Y-%m-%d')
    stock_list = g.security
    value = get_price(security=stock_list, end_date=today, frequency='daily',fields= ['paused', 'open','high_limit','low_limit'], count=1)
    stock_list = [stock for stock in stock_list if value['paused'].ix[0,stock] == 0]
    #stock_list = [stock for stock in stock_list if value['is_st'].ix[0,stock] == 0]
    stock_list = [stock for stock in stock_list if value['open'].ix[0,stock] != value['high_limit'].ix[0,stock] and value['open'].ix[0,stock]!= value['low_limit'].ix[0,stock]]
    log.info('股票池数量：%d' %len(stock_list))
    #获取财务数据
    q = query(
        valuation.code, 
        income.basic_eps, 
        valuation.pb_ratio,
        valuation.pe_ratio, 
        valuation.market_cap,
        income.operating_profit, 
        valuation.capitalization, 
        indicator.roe,
        balance.total_liability,
        indicator.inc_operation_profit_annual).filter(valuation.code.in_(stock_list)).order_by(
        valuation.code)
    df1 = get_fundamentals(q,date=yesterday)
    df1 = df1.set_index(df1['code'])

    df1 = df1[df1['roe'] > 0]
    
    df1['close'] = list([float("nan")] * (len(df1.index)))
    true_list = list(df1.index)
    close_p = history(security_list = true_list, field='close', count=1, unit='1d', skip_paused=False, fq='pre')
    for stock in true_list:
        df1.loc[stock, 'close'] = close_p[stock].values[0]   
    return df1
    
def Gscz(context,df1):
    df1['pps'] = df1['operating_profit'] / df1['capitalization']

    # 格氏成长公式
    df1['value'] = df1['pps'] * (27 + 2 * df1['inc_operation_profit_annual'])
    df1['outvalue_ratio'] = df1['value'] / df1['close']
    df1.dropna(inplace = True)
    df1 = df1.sort('outvalue_ratio', ascending=False)
    df1 = df1[:num3]
    buylist = list(df1['code'].values)
    return buylist
    
def time_select(context):
    yesterday = context.previous_date.strftime('%Y-%m-%d')
    qt = query(valuation.code, valuation.pb_ratio).filter()
    df = get_fundamentals(qt, date=yesterday)
    # 计算昨天市场所有股票PB值的分位数
    factor_quantiles = df.dropna().quantile([num4])
    PB = factor_quantiles.iloc[0].values 
    # 计算昨天市场上跌停的股票占比
    '''stock_list = list(get_all_securities('stock', yesterday).index)
    pct_all =history(security_list = stock_list, field='pre_close', count=5, unit='1d', skip_paused=False, fq='pre')
    pct_list = []
    for i in range(5):
        values = list(pct_all.iloc[0,i,])
        pct = (len([x for x in values if x <= -9.5])) / len(stock_list)
        pct_list.append(pct)
    pct = max(pct_list)
    log.info('上周最大跌停股票占比: %.2f%%' %(pct*100))'''
    
    # 择时空仓条件设置
    if PB >= 10:
        return num5, 'PB_long'
    #elif pct > 0.1:
        #return num6, False
    else:
        return 0, True