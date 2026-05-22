
# Challenge
1. se escoge el escenario B, fue el que me parecio mas interesante.
2. se elige azure, ya habia creado algunos recursos como azure functions.

# Nota: 
* dado que mis cuentas de correo ya no servian para el free trial de azure, se utilizo el siguiente correo Lauraocampo.bio@gmail.com

# RetailMax Data Platform — Documentación del Proyecto

Este documento tiene como finalidad explicar paso a paso la construcción del challenge técnico, incluyendo las decisiones de arquitectura, tecnologías utilizadas y procedimientos necesarios para replicar completamente el proyecto desde cero.

La idea es dividir el desarrollo en diferentes fases para mantener trazabilidad sobre cada componente implementado y facilitar futuras mejoras, mantenimientos o discusiones técnicas.

---

# Nota Importante sobre los nombres de recursos en Azure

Algunos recursos de Azure requieren nombres globalmente únicos, especialmente:

- Storage Accounts
- Azure SQL Server
- Key Vault
- Azure Data Factory

Por esta razón, si al desplegar el proyecto aparece un error indicando que el nombre ya existe, simplemente modifica el nombre del recurso manteniendo la misma estructura lógica del proyecto.



# Clonar el Repositorio

```bash
git clone https://github.com/jpap1021/dataknow-challenge-retail.git
cd dataknow-challenge-retail
```

---

# Fases del Proyecto

## Fase 1 — Generación de Data Fake y Carga Inicial

### Objetivo

En esta fase se construye la fuente de datos inicial del proyecto. Los objetivos principales son:

* Generar datasets retail realistas utilizando Python y Faker.
* Simular un entorno OLTP empresarial.
* Crear una base de datos origen en Azure SQL.
* Cargar la información generada hacia Azure.
* Preparar la base para la capa Bronze del Data Lake.

---

# Requerimientos

## 1. Python

Tener instalado:

* Python 3.10 o superior

Verificar instalación:

```bash
python --version
```

---

## 2. Cuenta de Azure

Se requiere una cuenta de Azure activa.

Verificar acceso utilizando Azure CLI:

```bash
az login
```

Verificar suscripción activa:

```bash
az account show
```

---

## 3. Crear infraestructura inicial en Azure

Crear el Resource Group:

```bash
az group create \
--name rg-retailmax-data-dev \
--location centralus
```

Crear Azure SQL Server:

```bash
az sql server create \
--name retailmaxsql4935 \
--resource-group rg-retailmax-data-dev \
--location centralus \
--admin-user <usuario> \
--admin-password <password>
```

Crear Azure SQL Database:

```bash
az sql db create \
--resource-group rg-retailmax-data-dev \
--server retailmaxsql4935 \
--name retailmaxdb \
--service-objective Basic
```

---

# Contexto de Arquitectura

Los recursos anteriores representan la fuente OLTP inicial del proyecto.

En esta base de datos se almacenará toda la información generada por los scripts de simulación antes de iniciar el pipeline analítico hacia Bronze, Silver y Gold.

---

# Configuración del Proyecto

## 1. Crear entorno virtual

Ubicarse en la raíz del proyecto:

```bash
cd <nombre-del-repositorio>
```

Crear el entorno virtual:

```bash
python -m venv venv
```

Activar entorno virtual:

### Linux / Mac

```bash
source venv/bin/activate
```

### Windows

```bash
venv\Scripts\activate
```

---

## 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

---

# Configuración de Variables de Entorno

Crear un archivo `.env` en la raíz del proyecto:

```env
AZURE_SQL_SERVER=retailmaxsql4935.database.windows.net
AZURE_SQL_DATABASE=retailmaxdb
AZURE_SQL_USERNAME=<usuario>
AZURE_SQL_PASSWORD=<password>
```

> Los nombres anteriores deben coincidir con los recursos creados previamente en Azure.

---

# Generación de Datos

## Ejecutar generación de datasets

```bash
python data-generation/main.py
```

Este proceso genera múltiples datasets retail en formatos:

* CSV
* Parquet

Los archivos generados incluyen:

* Clientes
* Productos
* Tiendas
* Ventas
* Inventario
* Proveedores
* Devoluciones

---

# Características de los Datos Generados

El generador fue diseñado para cumplir requerimientos similares a escenarios reales de negocio:

* Distribuciones estadísticas realistas.
* Horarios pico de ventas.
* Estacionalidad mensual.
* Integridad referencial.
* Valores nulos controlados.
* Simulación de comportamiento retail.

---

# Anomalías Intencionales

El proyecto también incluye anomalías controladas para pruebas de calidad de datos:

* Transacciones duplicadas.
* Fechas futuras.
* Clientes inválidos.
* Stock negativo.

Estas anomalías posteriormente serán detectadas o tratadas dentro del pipeline analítico.

---

# Carga hacia Azure SQL

Una vez generados los datasets, ejecutar:

```bash
python data-generation/load_to_azure_sql.py
```

Este script:

* Lee los archivos generados.
* Se conecta a Azure SQL Database.
* Inserta la información por chunks.
* Evita problemas de memoria y timeouts.

---

# Resultado Esperado

Al finalizar esta fase deberías tener:

✅ Datasets generados localmente
✅ Archivos CSV y Parquet creados
✅ Información cargada en Azure SQL
✅ Fuente OLTP lista para ingestión Bronze
✅ Datos con cobertura histórica de 12 meses
✅ Datos con anomalías controladas

---

# Nota Importante

Actualmente el repositorio ya contiene datasets previamente generados.

Por lo tanto, para realizar pruebas rápidas únicamente es necesario:

1. Crear el entorno virtual.
2. Configurar el archivo `.env`.
3. Ejecutar el script:

```bash
python data-generation/load_to_azure_sql.py
```

---

# Mejora Pendiente Detectada

Actualmente algunos generadores utilizan fechas hardcodeadas dentro del código.

La mejora pendiente consiste en parametrizar completamente las fechas utilizando el archivo `config.yaml`, permitiendo controlar dinámicamente:

* Fecha inicial
* Fecha final
* Cobertura histórica
* Estacionalidad temporal

# Fase 2 — Infraestructura como Código (Terraform)

---

## Objetivo

Automatizar la creación de la infraestructura cloud del proyecto utilizando Terraform, asegurando:

- Infraestructura reproducible
- Separación por ambientes
- Backend remoto para Terraform State
- Parametrización de recursos
- Buenas prácticas de seguridad
- Integración con Azure Databricks, Data Factory y Data Lake

---

# Arquitectura creada en Azure

Durante esta fase se crearon dos Resource Groups principales:

| Resource Group | Propósito |
|---|---|
| `rg-terraform-state` | Backend remoto para almacenar el Terraform State |
| `rg-retailmax-data-dev` | Plataforma analítica y procesamiento de datos |

---

# Paso 1 — Crear Backend Remoto para Terraform State

Antes de ejecutar Terraform, fue necesario crear manualmente la infraestructura mínima requerida para almacenar el estado remoto (`terraform.tfstate`).

Esto evita:
- guardar estados localmente,
- exponer información sensible,
- subir archivos `.tfstate` al repositorio.

---

## Crear Resource Group del backend

```bash
az group create \
  --name rg-terraform-state \
  --location centralus
```

```bash
az storage account create \
  --name stretailtfstatejpap \
  --resource-group rg-terraform-state \
  --location centralus \
  --sku Standard_LRS
```

```bash
az storage container create \
  --name tfstate \
  --account-name stretailtfstatejpap
```

# Paso 2 — Crear los recursos con Terraform

1. ejecutar los siguientes comandos: 
    terraform init
    terraform plan -var-file="dev.tfvars"
    terraform apply -var-file="dev.tfvars"

# Paso 3 - Revisar los recusros 

= ir a azure portal
- resource groups, debe existir rg-retailmax-data-dev
- deberian existir los siguientes recursos 

| Recurso                      | Propósito                               |
| ---------------------------- | --------------------------------------- |
| Azure Data Lake Storage Gen2 | Almacenamiento Bronze / Silver / Gold   |
| Azure Data Factory           | Orquestación de pipelines               |
| Azure Databricks             | Procesamiento distribuido con Spark     |
| Azure Key Vault              | Manejo seguro de secretos               |
| Log Analytics Workspace      | Monitoreo y observabilidad              |
| Action Group                 | Alertas y notificaciones                |
| Databricks Access Connector  | Conexión segura entre Databricks y ADLS |
| Role Assignments (RBAC)      | Gestión de permisos sobre recursos      |

# Fases 3,4:
en estas fases se podria integrar desde github mediante github action, hacer la configuracion repositorio databricks con la carpeta pipelines y adf con con la carpeta orchestation.

Metodo manual
1. lanza databricks
2. crea un cluster en databriocks en la parte de compute.
3. crea 3 notebooks en la parte de workspace
2. copia los archivos de la carpeta pipelines, 


# Data Factory
Para la parte de Data Factory lleva el archivo que esta en orchesattion, necesitas haber creado un linked service, en la parte arriba derecha en el simbolo json y cambian el nombre del pipeline, que coincida con tu nombre en azure y tu json, se creara la sequencia y configuracion que se creo originalmente 

# Fase 5
No se realizo en este proyecto

# Mejoras futuras
1. documentacion
2. completar la fase 5 del proyecto(roles y demas)
3. integrar con github action

