import pandas as pd
import random
from faker import Faker

fake = Faker("es_ES")

MOTIVOS = [
    "Producto defectuoso",
    "No era lo esperado",
    "Entrega tardía",
    "Error talla",
    "Producto incompleto"
]

ESTADOS = [
    "Aprobada",
    "Pendiente",
    "Rechazada"
]

def generate_devoluciones(
    ventas_df,
    total=50000
):

    devoluciones = []

    ventas_sample = ventas_df.sample(total)

    for i, (_, venta) in enumerate(ventas_sample.iterrows(), start=1):

        qty_devuelta = random.randint(1, venta["qty_vendida"])

        # manejar fechas futuras anómalas
        fecha_trans = pd.to_datetime(
            venta["fec_trans"]
        ).date()

        today = fake.date_object()

        if fecha_trans > today:
            fecha_trans = fake.date_between(
                start_date="-1y",
                end_date="today"
            )

        devolucion = {
            "id_devolucion": i,
            "id_trans_origen": venta["id_trans"],
            "art_id": venta["art_id"],
            "id_tienda": venta["id_tienda"],
            "fec_devolucion": fake.date_between(
                start_date=fecha_trans,
                end_date="today"
            ),
            "qty_devuelta": qty_devuelta,
            "motivo_cod": random.choice(MOTIVOS),
            "canal_devolucion": venta["canal_venta"],
            "estado_devolucion": random.choice(ESTADOS),
            "vr_reembolso": round(
                qty_devuelta *
                venta["precio_unitario_venta"],
                2
            )
        }

        devoluciones.append(devolucion)

    return pd.DataFrame(devoluciones)