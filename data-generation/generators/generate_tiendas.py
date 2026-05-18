import pandas as pd
import random
from faker import Faker

fake = Faker("es_ES")

CIUDADES = [
    ("Bogotá", "Colombia"),
    ("Medellín", "Colombia"),
    ("Cali", "Colombia"),
    ("Ciudad de México", "México"),
    ("Santiago", "Chile"),
    ("Lima", "Perú"),
    ("Quito", "Ecuador")
]

TIPOS_TIENDA = [
    "Hipermercado",
    "Supermercado",
    "Conveniencia"
]

def generate_tiendas(total=150):

    tiendas = []

    for i in range(1, total + 1):

        ciudad, pais = random.choice(CIUDADES)

        tienda = {
            "id_tienda": i,
            "nom_tienda": f"Tienda_{i}",
            "tipo_tienda": random.choice(TIPOS_TIENDA),
            "id_ciudad": ciudad,
            "id_pais": pais,
            "metros_cuadrados": random.randint(100, 10000),
            "activo": random.choice([True] * 9 + [False]),
            "fec_apertura": fake.date_between(
                start_date="-15y",
                end_date="today"
            )
        }

        tiendas.append(tienda)

    df = pd.DataFrame(tiendas)

    # nulos controlados
    null_indices = df.sample(frac=0.05).index
    df.loc[null_indices, "metros_cuadrados"] = None

    return df