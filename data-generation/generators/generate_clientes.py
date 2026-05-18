import pandas as pd
import random
from faker import Faker

fake = Faker("es_ES")

GENERO = ["M", "F", None]

CANALES = [
    "Web",
    "Mobile",
    "Tienda"
]

RANGOS_EDAD = [
    ("18-25", 0.20),
    ("26-35", 0.35),
    ("36-45", 0.25),
    ("46-60", 0.15),
    ("60+", 0.05)
]

def generate_clientes(total=50000):

    clientes = []

    for i in range(1, total + 1):

        fecha_registro = fake.date_between(
            start_date="-5y",
            end_date="today"
        )

        cliente = {
            "id_miembro": i,
            "fec_registro": fecha_registro,
            "id_ciudad": fake.city(),
            "genero": random.choice(GENERO),
            "rango_edad": random.choices(
                [r[0] for r in RANGOS_EDAD],
                weights=[r[1] for r in RANGOS_EDAD]
            )[0],
            "canal_pref": random.choice(CANALES),
            "activo": random.choice([True] * 9 + [False]),
            "fec_ultima_compra": fake.date_between(
                start_date=fecha_registro,
                end_date="today"
            )
        }

        clientes.append(cliente)


    df = pd.DataFrame(clientes)

    # 5% nulos controlados
    null_indices = df.sample(frac=0.05).index
    df.loc[null_indices, "canal_pref"] = None

    return df