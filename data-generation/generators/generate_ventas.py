import pandas as pd
import random
import numpy as np
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("es_ES")

CANALES = [
    "Web",
    "Mobile",
    "Tienda"
]

TIPOS_PAGO = [
    "Credito",
    "Debito",
    "Efectivo",
    "PSE"
]

HORAS_PICO = [10,11,12,18,19,20]

def random_hour():

    # 70% horas pico
    if random.random() < 0.7:
        hour = random.choice(HORAS_PICO)
    else:
        hour = random.randint(0,23)

    minute = random.randint(0,59)
    second = random.randint(0,59)

    return f"{hour:02}:{minute:02}:{second:02}"

def generate_ventas(
    clientes_df,
    tiendas_df,
    articulos_df,
    total=1000000
):

    ventas = []

    cliente_ids = clientes_df["id_miembro"].tolist()
    tienda_ids = tiendas_df["id_tienda"].tolist()

    articulos = articulos_df.to_dict("records")

    start_date = datetime(2025,1,1)
    end_date = datetime(2025,12,31)

    for i in range(1, total + 1):

        articulo = random.choice(articulos)

        meses_pesos = {
            1: 0.07,
            2: 0.06,
            3: 0.07,
            4: 0.07,
            5: 0.08,
            6: 0.08,
            7: 0.08,
            8: 0.08,
            9: 0.07,
            10: 0.09,
            11: 0.12,
            12: 0.13
        }

        mes = random.choices(
            list(meses_pesos.keys()),
            weights=list(meses_pesos.values())
        )[0]

        dia = random.randint(1, 28)

        fecha = datetime(2025, mes, dia)

        qty = np.random.poisson(2) + 1

        precio = articulo["precio_lista"]

        descuento = 0

        # 25% ventas con descuento
        if random.random() < 0.25:
            descuento = round(precio * random.uniform(0.05, 0.30), 2)

        venta = {
            "id_trans": i,
            "id_miembro": random.choice(cliente_ids),
            "id_tienda": random.choice(tienda_ids),
            "art_id": articulo["art_id"],
            "fec_trans": fecha,
            "hra_trans": random_hour(),
            "qty_vendida": qty,
            "precio_unitario_venta": precio,
            "descuento_aplicado": descuento,
            "tipo_pago": random.choice(TIPOS_PAGO),
            "canal_venta": random.choice(CANALES)
        }

        ventas.append(venta)

    df = pd.DataFrame(ventas)

    return df