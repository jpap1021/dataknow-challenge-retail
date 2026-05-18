import pandas as pd
import random
from faker_commerce import Provider
from faker import Faker

fake = Faker("es_ES")
fake.add_provider(Provider)

CATEGORIAS = {
    "Tecnología": ["Celulares", "TV", "Computadores"],
    "Hogar": ["Cocina", "Limpieza", "Decoración"],
    "Moda": ["Zapatos", "Camisas", "Pantalones"],
    "Bebés": ["Pañales", "Juguetes", "Ropa bebé"]
}

def generate_articulos(proveedores_df, total=5000):

    articulos = []

    proveedor_ids = proveedores_df["id_proveedor"].tolist()

    for i in range(1, total + 1):

        categoria_n1 = random.choice(list(CATEGORIAS.keys()))
        categoria_n2 = random.choice(CATEGORIAS[categoria_n1])

        articulo = {
            "art_id": i,
            "cod_barra": fake.ean(length=13),
            "desc_art": fake.ecommerce_name(),
            "id_categ_n1": categoria_n1,
            "id_categ_n2": categoria_n2,
            "id_categ_n3": fake.word(),
            "id_proveedor": random.choice(proveedor_ids),
            "precio_lista": round(random.uniform(5, 5000), 2),
            "peso_kg": round(random.uniform(0.1, 20), 2),
            "unid_medida": random.choice(["UND", "KG", "LT"]),
            "activo": True,
            "fec_alta": fake.date_between(
                start_date="-3y",
                end_date="today"
            )
        }

        articulos.append(articulo)

    return pd.DataFrame(articulos)