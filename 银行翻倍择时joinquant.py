from jqdata import *
#避免编辑器IDE报错
'''g,set_benchmark,set_option,log,set_order_cost,OrderCost,run_daily,send_message=0
query,valuation,get_fundamentals,order_target,order_target_value,get_trades,get_trade_days=0
run_monthly,get_iwencai,order_target_value,order_target_percent,get_datetime,get_price,history=0
get_last_datetime,income,income_one_season,growth_one_season,profit_one_season,balance=0;
get_concept_stocks=0'''

# 对比基准设置'801780.SL''000300.SH'
num1 = '801780.SL'
# 选股条件
#num2 = '银行行业 上市天数>200'
num2 = 'HY493'
# 目标选股数量
num3 = 3
#市场空仓对应的PB分位数阈值
num4 = 0.8
#长期空仓天数
num5 = 240
#中期空仓天数
num6 = 90
# 初始化函数，设定基准等等
def initialize(context):
    context.signal = True
    context.b = True
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
    #同花顺行业选取接口 get_iwencai(num2) 智能识别字符串,聚宽版本为get_concept_stocks
    g.security = get_industry_stocks(industry_code=num2, date=None)
    print g.security
    
    run_monthly(func=handle, monthday=10,time='open', reference_security = '000001.XSHG')
def handle(context,bar_dict=None):
    '''
    buylist = GrahamStockFilter(context)
    for hold in list(context.portfolio.positions.keys() ):
        if hold not in list(buylist):
            order_target_value(hold, 0)
    for stock in buylist:
        order_target_value(stock, context.portfolio.total_value*0.999/len(buylist) )
    '''
    
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
            
def GrahamStockFilter(context,overflow=0):
    #Graham 中国化 Yhfb（）
    df1 = Data(context)
    buylist = Yhfb(context,df1)
    log.info(context.current_dt.strftime('%Y-%m-%d') + '选股为:' + str(buylist)[:])
    return buylist
# 数据获取
def Data(context):
    yesterday = context.previous_date.strftime('%Y-%m-%d')
    today = context.current_dt.strftime('%Y-%m-%d')
    stock_list = g.security
    value = get_price(security=stock_list,end_date=today,  frequency='daily', \
        fields=['paused','open','high_limit','low_limit'],count=1)
    stock_list = [stock for stock in stock_list if value['paused'].ix[0,stock] == 0]
    #stock_list = [stock for stock in stock_list if value['is_st'].ix[0,stock] == 0]
    stock_list = [stock for stock in stock_list if value['open'].ix[0,stock] != value['high_limit'].ix[0,stock] and value['open'].ix[0,stock]!= value['low_limit'].ix[0,stock]]
    log.info('股票池数量：%d' %len(stock_list))
    #获取财务数据
    q = query(valuation.code, income.basic_eps, valuation.pb_ratio,valuation.pe_ratio, valuation.market_cap,
              income.operating_profit, valuation.capitalization, indicator.roe,balance.total_liability,
              indicator.inc_operation_profit_annual).filter(valuation.code.in_(stock_list)).order_by(
        valuation.code)
    df1 = get_fundamentals(q, date=yesterday)
    df1 = df1.set_index(df1['code'])
    df1 = df1[df1['roe'] > 0]
    df1['close'] = list([float("nan")] * (len(df1.index)))
    true_list = list(df1.index)
    close_p = history(security_list = true_list, field='close', count=1, unit='1d', skip_paused=False, fq='pre')
    for stock in true_list:
        df1.loc[stock, 'close'] = close_p[stock].values[0]   
    return df1

def Yhfb(context,df1):
    import math
    df1['double_time'] = df1.apply(
        lambda row: round(math.log(2.0 * row['pb_ratio'], (1.0 + row['roe'] / 100)), 2), axis=1)
    df_double = df1.sort('double_time', ascending=True)
    df1 = df_double[:num3]  
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