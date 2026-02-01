
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

from config import cfg
from data.fetch import get_coin_data
from utils.helpers import granularity_to_pandas_freq,generate_signal,compute_nav,forward_state

# ================= BACKTEST =================

def run_backtest_breakout(config):
    print("IMPORTING breakout_strat.py")
    strategy_name='breakout_5_20'
    coin_data = get_coin_data(config)

    # calculate rebalance date, frequency, and list of all dates
    #all_dates : datetime not panda timestamp
    gran_freq = granularity_to_pandas_freq(config.GRANULARITY)

    all_dates = pd.date_range(
        start=pd.Timestamp(config.START_DATE).normalize(),
        end=pd.Timestamp(config.END_DATE).normalize(),
        freq=gran_freq
    )

    if all_dates.empty:
        raise ValueError("Date grid empty. Check START_DATE / END_DATE / GRANULARITY.")

    rebalance_dates = pd.date_range(
    start=all_dates[0],
    end=all_dates[-1],
    freq=f"{config.FREQUENCY_DAYS}D"
    )

    close_df = pd.concat(
        {sym: df["close"] for sym, df in coin_data.items()},
        axis=1
    ).sort_index()

    logret = np.log(close_df).diff()

    # high_df = pd.concat(
    #     {sym: df["high"] for sym, df in coin_data.items()},
    #     axis=1
    # ).sort_index()
    # high_long = close_df.rolling(20, min_periods=20).max()
    # high_short  = close_df.rolling(5,  min_periods=5).max()

    # signal = (high_20 == high_5).astype(int)

    
    breakout_signal_dict=generate_breakout_signals(coin_data,5, 20)

def old_run_backtest_breakout(config):
    print("IMPORTING breakout_strat.py")
    strategy_name='breakout_5_20'

    coin_data = get_coin_data(config)
    
    print("Starting backtest...")
    strategy_data={}
    
    # calculate rebalance date, frequency, and list of all dates
    #all_dates : datetime not panda timestamp
    gran_freq = granularity_to_pandas_freq(config.GRANULARITY)
    all_dates = pd.date_range(start=config.START_DATE, end=config.END_DATE, freq=gran_freq).to_pydatetime().tolist()
    if not all_dates:
        raise SystemExit("[ERROR] Date grid empty. Check START_DATE/END_DATE/GRANULARITY.")
    #rebalance dates : datetime not panda timestamp
    rebalance_dates = pd.date_range(start=config.START_DATE, end=config.END_DATE, freq=f'{config.FREQUENCY_DAYS}D').to_pydatetime().tolist()
    #rebalance days : dic of panda timestamps rebalancing dates with index
    rebalance_day_to_idx = {
        pd.Timestamp(d).normalize(): i
        for i, d in enumerate(rebalance_dates)
    }
    rebalance_days = set(rebalance_day_to_idx)
    
    
    #adjust benchmark start date to first rebalance_date
    benchmark_start_date=rebalance_dates[1]
    benchmark_alloc={}
    
    strategy_data['strat'] = pd.DataFrame(index=all_dates).assign(
        nav=0.0,
        cash=0.0,
        nb_positions=0,
        opened_positions=0,
        closed_positions=0,
        total_purchases=0.0,
        total_sales=0.0,
        total_realized_pnl=0.0,
        total_positive_negative_close=0,
        benchmark_buy_and_hold=0.0,
        cummax=0.0
    )
    
    STRAT_STATE_COLS = [
    'cash',
    'nb_positions',
    ]

    STRAT_EVENT_COLS = [
        'opened_positions',
        'closed_positions',
        'total_purchases',
        'total_sales',
        'total_realized_pnl',
        'total_positive_negative_close',
    ]

    SYM_STATE_COLS = [
        'units',
        'purchase_price',
        'signal',
    ]

    SYM_EVENT_COLS = [
        'purchase',
        'sale',
        'realized_pnl',
    ]

    #initializes benchmark & positions
    #start date converted to Timestamp
    start_time=pd.Timestamp(config.START_DATE).normalize()
    strategy_data['strat'].loc[start_time, 'benchmark_buy_and_hold'] =config.INITIAL_CAPITAL

    for sym in coin_data:
        strategy_data[sym] = pd.DataFrame(index=all_dates).assign(
            close=coin_data[sym]['close'].reindex(all_dates).ffill().bfill(),  # reindex, drop different dates
            units=0.0,
            purchase=0.0,
            sale=0.0,
            purchase_price=0.0,
            realized_pnl=0.0,
            signal=""
        )
                
        #benchmark positions
        benchmark_alloc[sym]=(
            config.INITIAL_CAPITAL
            /len(coin_data)
            /strategy_data[sym].loc[benchmark_start_date,'close']
        )
        
        #initial signals
        strategy_data[sym].loc[start_time,'signal']=generate_signal(coin_data.get(sym), start_time)
        
        #initial allocation
        if strategy_data[sym].loc[start_time,'signal']=='LONG':
            strategy_data['strat'].loc[start_time, 'opened_positions'] +=1
            strategy_data['strat'].loc[start_time,'nb_positions'] +=1
            #equal weigh allocation of total cash            
            strategy_data[sym].loc[start_time,'purchase']=config.INITIAL_CAPITAL/len(coin_data)
            strategy_data[sym].loc[start_time,'purchase_price']=strategy_data[sym].loc[start_time,'close']*1/(1-config.FEE)
            strategy_data[sym].loc[start_time,'units']=(
                strategy_data[sym].loc[start_time,'purchase']
                /strategy_data[sym].loc[start_time,'purchase_price']
            )
            strategy_data['strat'].loc[start_time,'total_purchases']+=strategy_data[sym].loc[start_time,'purchase']

    #initializes cash & nav
    strategy_data['strat'].loc[start_time,'cash']= (
        config.INITIAL_CAPITAL
        -strategy_data['strat'].loc[start_time,'total_purchases']
    )
    strategy_data['strat'].loc[start_time, 'nav']=compute_nav(strategy_data, coin_data, start_time)

    # SIMULATION
    for prev_dt, current_dt in zip(all_dates[:-1], all_dates[1:]):
        
        #state at start (incl non trading days)
        current_day = pd.to_datetime(current_dt).normalize()
        forward_state(strategy_data['strat'], STRAT_STATE_COLS, STRAT_EVENT_COLS, prev_dt, current_dt)
        for sym in coin_data:
            df_sym = strategy_data[sym]
            forward_state(df_sym,SYM_STATE_COLS,SYM_EVENT_COLS,prev_dt, current_dt)
        strategy_data['strat'].loc[current_dt, 'nav'] =compute_nav(strategy_data, coin_data, current_dt)
        strategy_data['strat'].loc[current_dt, 'benchmark_buy_and_hold'] =(
            sum(
                strategy_data[sym].loc[current_dt, 'close']
                *benchmark_alloc[sym]
                *(current_dt>=benchmark_start_date)
                for sym in coin_data
            )
            +(current_dt<benchmark_start_date)*config.INITIAL_CAPITAL
        ) 
                    
        # select rebalance days
#         if any(pd.to_datetime(current_dt).normalize() == pd.to_datetime(d).normalize() for d in rebalance_dates):
        if current_day in rebalance_days:
            reb_idx = rebalance_day_to_idx[current_day]
#             reb_idx = [pd.to_datetime(d).normalize() for d in rebalance_dates].index(pd.to_datetime(current_dt).normalize())
            pct_reb = (reb_idx+1) / max(1, len(rebalance_dates)) * 100
            print(f"[{pct_reb:.1f}%] Rebalance date: {current_day.date()}")
            
            #calculates which coins to buy/sell- step 1
            to_close = []
            to_open = []
            for sym in coin_data:
                df_sym = strategy_data[sym]
                prev_signal = df_sym.loc[prev_dt, 'signal']
                curr_signal = generate_signal(coin_data.get(sym), current_dt)
                df_sym.loc[current_dt, 'signal'] = curr_signal
                if prev_signal == 'LONG' and curr_signal == 'FLAT':
                    to_close.append(sym)
                elif prev_signal == 'FLAT' and curr_signal == 'LONG':
                    to_open.append(sym)

            #generate all sales- step 2
            for sym in to_close:
                df_sym = strategy_data[sym]
                df_sym.loc[current_dt,'sale']= (
                    df_sym.loc[prev_dt,'units']
                    *df_sym.loc[current_dt,'close']
                    *(1-config.FEE)
                )
                cost_of_acquisition=df_sym.loc[prev_dt,'purchase_price']*df_sym.loc[prev_dt,'units']
                realized_pnl=(
                    df_sym.loc[current_dt,'sale']
                    -cost_of_acquisition
                )
                df_sym.loc[current_dt,'realized_pnl']=realized_pnl    
                strategy_data['strat'].loc[current_dt,'total_realized_pnl']+=realized_pnl
                sign = '+' if realized_pnl >= 0 else ''
                print(f"📉 Closed position on {sym} | PnL: {sign}{realized_pnl:.2f} USD ({sign}{(realized_pnl/cost_of_acquisition*100):.1f})%")
                strategy_data['strat'].loc[current_dt,'total_positive_negative_close']+=np.sign(df_sym.loc[current_dt,'realized_pnl'])
                strategy_data['strat'].loc[current_dt,'total_sales']+=df_sym.loc[current_dt,'sale'] 
                strategy_data[sym].loc[current_dt,'units']=0
                strategy_data['strat'].loc[current_dt,'closed_positions'] +=1
                strategy_data['strat'].loc[current_dt,'nb_positions'] -=1     
            
            #calculates cash from all sales- step 3
            cash_available=strategy_data['strat'].loc[current_dt,'cash']+strategy_data['strat'].loc[current_dt,'total_sales']
            if strategy_data['strat'].loc[current_dt,'closed_positions']>0:
                print(f"[INFO] Closed {strategy_data['strat'].loc[current_dt,'closed_positions']} position(s)")         

            #calculates allocation for new signals - step 4
            # method pro rata nb active signals
            if config.REBALANCING=='prorata_active':
                #no rebalancing of existing positions
                #weight per new signal= 1 / (nb coins - (nb previous positions - closed position))            
                #total slots : nb coins
                #taken slots : nb previous positions - closed positions
                #available slots : nb coins - (nb previous positions - closed position)
                #calculates new/current positions for cash allocation - step 3a  
                for sym in to_open:
                    strategy_data['strat'].loc[current_dt,'opened_positions'] +=1
                    strategy_data['strat'].loc[current_dt,'nb_positions'] +=1            
                
                if strategy_data['strat'].loc[current_dt,'opened_positions']>0:
                    weight_per_new_signal=(1/                                           
                                           (len(coin_data)
                                            -strategy_data['strat'].loc[prev_dt,'nb_positions']
                                            +strategy_data['strat'].loc[current_dt,'closed_positions']
                                           )
                    )
                else:weight_per_new_signal=0
            
            #calculates allocation for new signals if not prorata method- step 4 alternative
            # method full alloc on active signals
            elif config.REBALANCING=='full_active':
                #no rebalancing of existing positions
                #weight per new signal= 1/ nb new signal
                #calculates new/current positions for cash allocation 
                if cash_available>0:
                    for sym in to_open:
                        strategy_data['strat'].loc[current_dt,'opened_positions'] +=1
                        strategy_data['strat'].loc[current_dt,'nb_positions'] +=1 
            
                if strategy_data['strat'].loc[current_dt,'opened_positions']>0:
                    weight_per_new_signal=1/strategy_data['strat'].loc[current_dt,'opened_positions']
                else: weight_per_new_signal=0
                    
            #calculates cash alloc - step 5
            alloc_per_new_signal = cash_available*weight_per_new_signal

            #generate all purchases- step 6
            for sym in to_open:
                df_sym = strategy_data[sym]
                close = df_sym.loc[current_dt, 'close']
                df_sym.loc[current_dt,'purchase_price'] = close / (1 - config.FEE)
                df_sym.loc[current_dt,'units'] = alloc_per_new_signal /  df_sym.loc[current_dt,'purchase_price']
                df_sym.loc[current_dt,'purchase']=alloc_per_new_signal
                strategy_data['strat'].loc[current_dt,'total_purchases']+=df_sym.loc[current_dt,'purchase']
                print(f"[INFO] Opened new position on {sym}")
                        
            #updates cash- step 6
            strategy_data['strat'].loc[current_dt,'cash']=(
                cash_available
                -strategy_data['strat'].loc[current_dt,'total_purchases']
            )
            
            #updates nav- step 7
            strategy_data['strat'].loc[current_dt, 'nav'] =compute_nav(strategy_data, coin_data, current_dt)
 
    return strategy_name,coin_data,strategy_data

