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
    "18-25",
    "26-35",
    "36-45",
    "46-60",
    "60+"
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
            "rango_edad": random.choice(RANGOS_EDAD),
            "canal_pref": random.choice(CANALES),
            "activo": random.choice([True] * 9 + [False]),
            "fec_ultima_compra": fake.date_between(
                start_date=fecha_registro,
                end_date="today"
            )
        }

        clientes.append(cliente)

    df = pd.DataFrame(clientes)

    return df