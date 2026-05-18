import pandas as pd
import random
from faker import Faker

fake = Faker("es_ES")

PAISES = [
    "Colombia",
    "México",
    "Chile",
    "Perú",
    "Brasil",
    "Argentina",
    "China",
    "USA"
]

def generate_proveedores(total=800):

    proveedores = []

    for i in range(1, total + 1):

        proveedor = {
            "id_proveedor": i,
            "razon_social": fake.company(),
            "pais_origen": random.choice(PAISES),
            "tiempo_repo_dias": random.randint(1, 30),
            "calificacion_calidad": round(random.uniform(1, 5), 2),
            "activo": random.choice([True] * 9 + [False])
        }

        proveedores.append(proveedor)

    df = pd.DataFrame(proveedores)

    # 5% nulos controlados
    null_indices = df.sample(frac=0.05).index
    df.loc[null_indices, "calificacion_calidad"] = None

    return df