import pandas as pd
import random
import numpy as np

from faker import Faker
from datetime import datetime

fake = Faker()

def generate_stock(
    articulos_df,
    tiendas_df,
    total=750000
):

    snapshots = []

    articulos_ids = articulos_df["art_id"].tolist()
    tiendas_ids = tiendas_df["id_tienda"].tolist()

    start_date = datetime(2025,1,1)
    end_date = datetime(2025,12,31)

    for i in range(1, total + 1):

        stock_fisico = random.randint(0,500)

        snapshot = {
            "id_snapshot": i,
            "art_id": random.choice(articulos_ids),
            "id_tienda": random.choice(tiendas_ids),
            "fec_snapshot": fake.date_between_dates(
                date_start=start_date,
                date_end=end_date
            ),
            "stock_fisico" = max(0, int(np.random.normal(120, 60))),
            "stock_transito": random.randint(0,100),
            "stock_reservado": random.randint(0,50),
            "stock_minimo_config": random.randint(10,50),
            "stock_maximo_config": random.randint(100,1000)
        }

        snapshots.append(snapshot)

    return pd.DataFrame(snapshots)