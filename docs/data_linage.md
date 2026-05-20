# Data Lineage

## vr_venta_neto
- Origen: TRANS_VENTAS
- Transformación:
  qty_vendida * precio_unitario_venta - descuento_aplicado
- Propósito:
  Calcular ventas netas reales.

## cobertura_dias
- Origen: INV_STOCK_DIARIO
- Transformación:
  stock_fisico / promedio_consumo_14dias
- Propósito:
  Detectar riesgo de quiebre de inventario.

## segmento_rfm
- Origen:
  TRANS_VENTAS + CRM_MIEMBROS
- Transformación:
  Scoring por quintiles RFM
- Propósito:
  Segmentación comercial de clientes.