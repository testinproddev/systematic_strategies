import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from pathlib import Path

from config import config


def metrics(strategy_name,coin_data,strategy_data):
    # metrics
    risk_free_rate=0.05
    metrics = pd.DataFrame([{
            'strategy name':strategy_name,
            'annualized return':0.0,
            'annualized vol':0.0,
            'sharpe ratio':0.0,
            'max drawdown':0.0,
            'average realized pnl':0.0,
            'hit rate':0.0,
            'benchmark ann return':0.0,
            'benchmark ann vol':0.0,
            'benchmark sharpe':0.0,
            'benchmark drawdown':0.0,
            'average cash allocation':0.0,
            'nb coins':len(coin_data),
            'coins': list(coin_data.keys()),
            'nb opened':0,
            'nb closed':0,
            'nb_current_positions':0,
            'start date':config.START_DATE,
            'last date':config.END_DATE,
            'nb days':0
    }]).set_index('strategy name')
    metrics['nb opened']=strategy_data['strat']['opened_positions'].sum()
    metrics['nb closed']=strategy_data['strat']['closed_positions'].sum()
    #calculates hit rate with still opened positions
    nb_current_positions=0
    nb_unrealized_wins=0
    for sym in coin_data:
        #iloc[-2] : second to last row
        if strategy_data[sym]['sale'].iloc[-2]==0 and strategy_data[sym]['units'].iloc[-2]>0:
            u_pnl_sym=(
                strategy_data[sym]['units'].iloc[-2]
                *(strategy_data[sym]['close'].iloc[-2]-strategy_data[sym]['purchase_price'].iloc[-2])
            )
            unrealized_pnl=+u_pnl_sym
            nb_current_positions+=1
            if u_pnl_sym>0:
                nb_unrealized_wins+=1
            elif u_pnl_sym<0:
                nb_unrealized_wins-=1
    metrics['nb_current_positions']=nb_current_positions
    nb_wins=(
        nb_unrealized_wins
        +strategy_data['strat'].loc[strategy_data['strat']['total_positive_negative_close'] == 1,'total_positive_negative_close'
        ].sum()
    )
    if strategy_data['strat']['closed_positions'].sum()>0:
        metrics['hit rate']=nb_wins/(nb_current_positions +strategy_data['strat']['closed_positions'].sum())
        metrics['average realized pnl']=(
            strategy_data['strat']['total_realized_pnl'].sum()
            /strategy_data['strat']['closed_positions'].sum()
        )
    else :
        metrics['hit rate']=0
        metrics['average realized pnl']=0

    days = (strategy_data['strat'].index[-2] - strategy_data['strat'].index[0]).days \
                                if len(strategy_data['strat'].index)>2 else 0
    years = days / 365.25 if days > 0 else 1.0
    metrics['nb days']=days
    metrics['annualized return']=(
        (strategy_data['strat']['nav'].iloc[-2]/config.INITIAL_CAPITAL)
        **(1.0/years)-1
    )
    metrics['benchmark ann return']=(
        (strategy_data['strat']['benchmark_buy_and_hold'].iloc[-2]/config.INITIAL_CAPITAL)
        **(1.0/years)-1
    )
    log_returns = np.log(strategy_data['strat']['nav'].ffill()).diff().fillna(0)
    log_returns_benchmark = np.log(strategy_data['strat']['benchmark_buy_and_hold'].ffill()).diff().fillna(0)
    metrics['annualized vol']=log_returns.std() *np.sqrt(252) if len(log_returns)>1 else 0.0 
    metrics['benchmark ann vol']=log_returns_benchmark.std() *np.sqrt(252) if len(log_returns_benchmark)>1 else 0.0 
    metrics['sharpe ratio'] = (
        (metrics['annualized return']-risk_free_rate)
        /metrics['annualized vol']
    )
    metrics['benchmark sharpe'] = (
        (metrics['benchmark ann return']-risk_free_rate)
        /metrics['benchmark ann vol']
    )
    strategy_data['strat']['cummax'] = strategy_data['strat']['nav'].cummax()
    drawdown=-(strategy_data['strat']['nav'].cummax()-strategy_data['strat']['nav'])
    metrics['max drawdown']=drawdown.min() if not drawdown.empty else 0.0
    metrics['average cash allocation'] = (
        strategy_data['strat']['cash'].sum()
        /strategy_data['strat']['nav'].sum()
    )
    bmk_drawdown=-(strategy_data['strat']['benchmark_buy_and_hold'].cummax()-strategy_data['strat']['benchmark_buy_and_hold'])
    metrics['benchmark drawdown']=bmk_drawdown.min() if not bmk_drawdown.empty else 0.0

    #metrics
    percent_cols = ['annualized return'
            ,'annualized vol'
            ,'hit rate'
            ,'benchmark ann return'
            ,'benchmark ann vol'
            ,'average cash allocation'
                   ]
    
    """  #Jupyter only - display
        with pd.option_context(
        'display.max_rows', None,
        'display.max_columns', None
    ):
        display(
            strategy_data['metrics']
            .style
            .format(
                {col: '{:.1%}' for col in percent_cols},
                precision=2
            )
        )
     
    #current signals
    current_signals = {}
    for sym in coin_data:
        current_signals[sym] = generate_signal(coin_data[sym], END_DATE)
    df = pd.DataFrame.from_dict(
    current_signals,
    orient='index',
    columns=['Current Signals']
    )
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        display(df) """
    
    print(metrics)
    
    # export in excel
    if config.EXPORT_DATA:
        print("Exporting Excel report: backtest_report.xlsx ...")
        try:
            with pd.ExcelWriter("backtest_report_bo520_cash.xlsx", engine="openpyxl") as writer:
                strategy_data['strat'].to_excel(writer, sheet_name="Strat Data", index=True)
                metrics.to_excel(writer, sheet_name="Metrics", index=True)
                for sym in coin_data:
                    strategy_data[sym].to_excel(writer, sheet_name=sym, index=True)
#                     coin_data[sym].to_excel(writer, sheet_name=f"Coin Data {sym}", index=True)
            print("Excel saved: backtest_report_alpha.xlsx")
        except Exception as e:
            print(f"[ERROR] Excel export failed: {e}")



def plot(strategy_data):
    
    # plots
    plt.figure(figsize=(12,6))
    plt.plot(strategy_data['strat'].index, strategy_data['strat']['nav'], label='Nav')
    plt.plot(strategy_data['strat'].index, strategy_data['strat']['cummax'], label='Cumulative Max', linestyle='--')
    plt.plot(strategy_data['strat'].index, strategy_data['strat']['benchmark_buy_and_hold'], label='Buy and hold', color='gray')
    plt.fill_between(strategy_data['strat'].index, strategy_data['strat']['cummax'], strategy_data['strat']['nav'], color='red', alpha=0.2, label='Drawdown')
    plt.title("Nav vs benchmark & drawdowns")
    plt.xlabel("Date")
    plt.ylabel("Nav")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()
