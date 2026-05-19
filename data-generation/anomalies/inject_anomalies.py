import pandas as pd
import random
from datetime import datetime, timedelta

def inject_duplicate_sales(df_ventas):

    duplicates = df_ventas.sample(frac=0.01)

    df_result = pd.concat(
        [df_ventas, duplicates],
        ignore_index=True
    )

    return df_result

def inject_future_dates(df_ventas):

    sample_idx = df_ventas.sample(frac=0.005).index

    future_date = pd.Timestamp.now() + timedelta(days=365)

    df_ventas.loc[sample_idx, "fec_trans"] = future_date

    return df_ventas

def inject_invalid_customer(df_ventas):

    sample_idx = df_ventas.sample(frac=0.005).index

    df_ventas.loc[sample_idx, "id_miembro"] = 999999999

    return df_ventas

def inject_negative_stock(df_stock):

    sample_idx = df_stock.sample(frac=0.01).index

    df_stock.loc[sample_idx, "stock_fisico"] = -10

    return df_stock