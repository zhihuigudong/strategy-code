# 导入函数库
from jqdata import *

# 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 输出内容到日志 log.info()
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')

    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')

    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
      # 开盘前运行
    run_daily(before_market_open, time='before_open', reference_security='000300.XSHG')
      # 开盘时运行
    run_daily(market_open, time='open', reference_security='000300.XSHG')
      # 收盘后运行
    run_daily(after_market_close, time='after_close', reference_security='000300.XSHG')
    
    #全局变量
    g.date='' #空仓到期日
    g.half_date='' #半仓到期日
    g.buy=1 #可买入信号
    g.short=0 #空仓信号
    g.half_short=0 #半仓信号

## 开盘前运行函数
def before_market_open(context):
    pass

## 开盘时运行函数
def market_open(context):
    today=context.current_dt.date()
    '''
    #可买入且不在空仓期
    if g.buy == 1 and g.short == 0:
        order_target_value('000300.XSHG',context.portfolio.total_value)
    #若触发空仓信号
    if pb_all_market_short(today):
        g.date=today + datetime.timedelta(days=250) 
        g.buy=0 #关闭可买入信号
        g.short=1 #开启空仓信号
        order_target_value('000300.XSHG',0) #清仓
        
    if  g.buy == 0:
        #空仓期结束
        if g.short == 1 and today > g.date:
            g.buy=1 #打开可买入信号
            g.short=0 #关闭空仓信号
    '''
    
    pb_all_short=pb_all_market_short(today) #全市场空仓信号
    pb_300_short=pb_hs300_short(today) #沪深300空仓信号
    
    #可买入且不在空仓期
    if g.buy == 1 and g.short == 0 and g.half_short == 0:
        order_target_value('000300.XSHG',context.portfolio.total_value)
    #若触发空仓信号1
    if pb_300_short == 0:
        g.date=today + datetime.timedelta(days=250) #空仓到期日
        g.buy=0 #关闭可买入信号
        g.short=1 #开启空仓信号
        print("触发空仓信号1啦！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！")
        order_target_value('000300.XSHG',0) #清仓
    #若触发空仓信号2
    if g.buy == 1 and pb_all_short == 0:
        g.date=today + datetime.timedelta(days=250) #空仓到期日
        g.buy=0 #关闭可买入信号
        g.short=1 #开启空仓信号
        print("触发空仓信号2啦！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！")
        order_target_value('000300.XSHG',0) #清仓
    
    if g.short == 0:
        pb_long=long_pb(today) #pb斜率牛市确认信号
        #若出现半仓信号1
        if pb_300_short == 0.5 and pb_long == 0:
            g.half_date=today + datetime.timedelta(days=125) #半仓到期日
            g.half_short=1 #开启半仓信号
            print("触发半仓信号1啦！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！")
        #若出现半仓信号2
        if pb_all_short == 0.5 and pb_long != 1:
            g.half_date=today + datetime.timedelta(days=250) #半仓到期日
            g.half_short=1 #开启半仓信号
            print("触发半仓信号2啦！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！")
    #保持半仓
    if g.half_short == 1 and g.buy == 1:
        order_target_value('000300.XSHG',context.portfolio.total_value/2) #半仓
    #空仓期结束
    if  g.buy == 0:
        if g.short == 1 and today > g.date:
            g.buy=1 #打开可买入信号
            g.short=0 #关闭空仓信号
    #半仓期结束
    if g.half_short == 1 and today > g.half_date:
        g.half_short = 0 #关闭半仓信号

## 收盘后运行函数
def after_market_close(context):
    pass

#沪深300pb阈值仓位管理信号
def pb_hs300_short(today):
    stock_list=get_index_stocks('000300.XSHG',date=today)
    df = get_fundamentals(query(valuation.pb_ratio).filter(valuation.code.in_(stock_list)),date=today)
    if len(df)!=0:
        pb=df.quantile(0.95)[0]
    if pb >= 15:
        return 0
    elif pb < 15 and pb >= 10:
        return 0.5
    else:
        return 1

#h全市场pb阈值空仓信号
def pb_all_market_short(date):
    stock_list=get_all_securities(date=date).index.tolist()
    df = get_fundamentals(query(valuation.pb_ratio).filter(valuation.code.in_(stock_list)),date=date)
    if len(df)!=0:
        pb=df.quantile(0.95)[0]
    if pb >= 20:
        return 0
    elif pb < 20 and pb > 15:
        return 0.5
    else:
        return 1

#牛市确认信号（pb斜率）
def long_pb(date):
    trade_days=get_trade_days(end_date=date,count=200)
    pb=[]
    for i in range(len(trade_days)):
        stock_list=get_index_stocks('000300.XSHG',date=trade_days[i])
        df = get_fundamentals(query(valuation.pb_ratio).filter(valuation.code.in_(stock_list)),date=trade_days[i])
        if len(df)!=0:
            pb.append(df.quantile(0.95)[0])
    pb_diff=[]
    pb_diff_sum=[]
    for i in range(len(pb)):
        if i >= 125:
            pb_diff.append(pb[i]-pb[i-125])
    for i in range(len(pb_diff)):
        if i >= 63:
            pb_diff_sum.append(sum(pb_diff[i-63:i]))
    print(pb_diff_sum[-1])
    if pb_diff_sum[-1] > 100:
        return 1
    elif pb_diff_sum[-1] > 50 and pb_diff_sum[-1] < 100:
        return 0.5
    else:
        return 0
#一阶导函数
def fun1(X, WINDOW):
    result = []
    for k in range(WINDOW, len(X)):
        mid = (X[k]-X[k-WINDOW])/(WINDOW)
        result.append(mid)
    return result

#净流入资金累积信号
def mon_flow_signal(context):
    today=context.current_dt.date()
    yes=today-datetime.timedelta(1)
    trade_days=get_trade_days(end_date=yes,count=45)
    mon_flow=[]
    mon_10=[]
    for i in range(len(trade_days)):
        stock_list=get_index_stocks('000300.XSHG',date=trade_days[i])
        pct_all =get_price(stock_list,end_date=trade_days[i],fields=['pre_close','close','money'], count=1, frequency='daily', skip_paused=False,fq='pre')
        percent_yes_df = (pct_all['close']-pct_all['pre_close'])/pct_all['pre_close']*100
        for j in range(len(percent_yes_df)):
            if percent_yes_df.ix[0,j] > 0:
                percent_yes_df.ix[0,j] = 1
            elif percent_yes_df.ix[0,j] == 0:
                percent_yes_df.ix[0,j] = 0
            else:
                percent_yes_df.ix[0,j] = -1
        money=(pct_all['money'].iloc[0,:]*percent_yes_df.iloc[0,:]).sum()
        mon_flow.append(money)
    for i in range(len(mon_flow)):
        if i >= 10:
            mon_10.append(sum(mon_flow[i-10:i]))
    if mon_10[-1] >= 0:
        return True
    if  np.mean(mon_10[-7:]) < 0 and mon_10[-1] >  np.mean(mon_10[-7:])/2:
        return True
    if mon_10[-1] <= np.mean(mon_10[-7:])/2 and np.mean(mon_10[-20:]) > 5*1e10:
        return False
    
