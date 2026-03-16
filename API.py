import requests
import pandas as pd

# --- Step 1: Extract ---
def fetch_crypto_data(asset, interval):
    endpoint = f"https://data-api.coindesk.com/spot/v1/historical/{interval}s"
    
    headers = {
        "Content-type": "application/json; charset=UTF-8",
        "authorization": "Apikey 8d3aad35b5bca4b8716f3502bd74e04918a2bcc7d07e9f4cff43f3774ccc09aa"
    }
    
    params = {
        "market": "kraken",
        "instrument": f"{asset}-USD",
        "limit": 2000,
        "groups": "OHLC_TRADE,OHLC,TRADE,VOLUME",
        "apply_mapping": "true",
        "response_format": "JSON"
    }
    
    response = requests.get(endpoint, params=params, headers=headers)
    json_data = response.json()
    
    if 'Data' not in json_data or not json_data['Data']:
        return pd.DataFrame()
    
    df = pd.DataFrame(json_data['Data'])
    df.columns = [col.upper() for col in df.columns]
    
    # Handle the date column
    ts_col = 'TIMESTAMP' if 'TIMESTAMP' in df.columns else 'TS'
    df['DATE_CLEAN'] = pd.to_datetime(df[ts_col], unit='s')
    
    return df

# --- Step 2: Transform ---
def backtest_strategy(df):
    if df.empty:
        return 0, "N/A", "N/A", 0

    # Ensure we use the correct trade columns we saw in your terminal
    t_buy = 'TOTAL_TRADES_BUY' if 'TOTAL_TRADES_BUY' in df.columns else 'TRADES_BUY'
    t_sell = 'TOTAL_TRADES_SELL' if 'TOTAL_TRADES_SELL' in df.columns else 'TRADES_SELL'

    # Metric 1: Volume Imbalance
    df['VIMB'] = (df['VOLUME_BUY'] - df['VOLUME_SELL']) / (df['VOLUME_BUY'] + df['VOLUME_SELL'] + 0.000001)
    
    # Metric 2: Trade Dominance Ratio
    df['TDR'] = df[t_buy] / (df[t_sell] + 1)
    
    # Metric 3: True Range Volatility Score
    df['TRVS'] = (df['HIGH'] - df['LOW']) / (df['OPEN'] + 0.000001)
    
    total_pnl = 0
    in_position = False
    buy_price = 0
    candles_held = 0
    trades_executed = 0

    for i in range(1, len(df)):
        row = df.iloc[i]
        prev_row = df.iloc[i-1]
        
        if not in_position:
            # Entry Conditions
            c1 = row['VIMB'] > 0
            c2 = row['TDR'] >= 0.7
            c3 = row['TRVS'] > 0.001
            c4 = row['CLOSE'] > row['OPEN']
            
            if c4 and (int(c1) + int(c2) + int(c3)) >= 2:
                in_position = True
                buy_price = row['CLOSE']
                candles_held = 0
                trades_executed += 1
        else:
            candles_held += 1
            # Exit Conditions
            exit_trigger = (
                row['VIMB'] < 0 or 
                row['TRVS'] < 0.0005 or 
                candles_held >= 5 or 
                (row['CLOSE'] < row['OPEN'] and prev_row['CLOSE'] < prev_row['OPEN'])
            )
            
            if exit_trigger:
                total_pnl += (row['CLOSE'] - buy_price)
                in_position = False
                
    return total_pnl, df['DATE_CLEAN'].iloc[0], df['DATE_CLEAN'].iloc[-1], trades_executed

# --- Step 3: Load ---
choice = input("Choose your trading asset (ETH or BTC): ").upper()

# Run backtests
pnl_h, start_h, end_h, count_h = backtest_strategy(fetch_crypto_data(choice, "hour"))
pnl_d, start_d, end_d, count_d = backtest_strategy(fetch_crypto_data(choice, "day"))

# Final Output String
print(f"\nYou have chosen the {choice}-USD pair.")
print(f"The backtest is run on the Hourly data from {start_h} to {end_h}. This gives {pnl_h:.2f} in profit/loss!")
print(f"The backtest on the Daily data from {start_d} to {end_d}, gives {pnl_d:.2f} in profit/loss!")

winner = "Hourly" if pnl_h > pnl_d else "Daily"
print(f"Therefore the {winner} trading is better!")