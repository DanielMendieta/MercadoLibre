#1-IMPORTO Y PREPARO TODO LO QUE VOY A NECESITAR A LO LARGO DEL CODIGO.
import requests
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import smtplib
from email.message import EmailMessage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Configuración de fecha y hora actual
fecha_hora_actual = datetime.now()
fechaHoraActual = fecha_hora_actual.strftime("%d/%m/%Y %H:%M")

#2-Función para la extracción de la API de Mercado Libre
def extraer_datos_mercado_libre():
    url = "https://api.mercadolibre.com/sites/MLA/search?category=MLA1577#json"
    response = requests.get(url).text
    response = json.loads(response)
    data = response["results"]
    tabla = pd.DataFrame(data)
    # Selecciono solo las columnas que me interesan.
    columnas_seleccionadas = ["id", "site_id", "title", "price", "sold_quantity", "thumbnail_id"]
    data = tabla[columnas_seleccionadas]
    tabla = data
    # Agrego la columna "fecha_Hora"
    data["fecha_Hora"] = fechaHoraActual
    return tabla

#3-Función para conexión y creación de la tabla en Amazon Redshift
def crear_conexion_insert(tabla):
    try:
        conn = psycopg2.connect(
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            dbname="data-engineer-database",
            user="mendietadaniel1994_coderhouse",
            password='XXXXX',
            port='5439'
        )
        print("Conexión Exitosa.")
        cur = conn.cursor()
        
        dtypes = tabla.dtypes
        columnas = list(dtypes.index)
        tipos = list(dtypes.values)
        conversorDetipos = {'int64': 'INT', 'int32': 'INT', 'float64': 'FLOAT', 'object': 'VARCHAR(100)', 'bool': 'BOOLEAN'}
        sql_dtypes = [conversorDetipos[str(dtype)] for dtype in tipos]
        column_defs = [f"{name} {data_type}" for name, data_type in zip(columnas, sql_dtypes)]
        sql = f"CREATE TABLE if not exists Microondas_ML ({', '.join(column_defs)}, PRIMARY KEY (id));"
        # Especificar que la columna "id" no permite valores nulos
        sql_dtypes[columnas.index("id")] += " NOT NULL"    
        cur.execute(sql)

        # Inserción de datos en la tabla
        values = [tuple(x) for x in tabla.to_numpy()]
        sqll = f"INSERT INTO Microondas_ML ({', '.join(columnas)}) VALUES %s"
        execute_values(cur, sqll, values)
        
        conn.commit()
        print("Tabla creada y datos ingresados.")

    except Exception as e:
        print("Algo salió mal.", str(e))
    finally:
        conn.close()
        print("Conexión Terminada.")

#4-Función para realizar la consulta y obtener los datos que necesitas
def consulta_email():
    try:
        conn = psycopg2.connect(
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            dbname="data-engineer-database",
            user="mendietadaniel1994_coderhouse",
            password='XXXXX',
            port='5439'
        )
        print("Conexión Exitosa.")
        cur = conn.cursor()
        ventas_sql = f"SELECT * FROM Microondas_ML WHERE sold_quantity * price > 10000000;"
        cur.execute(ventas_sql)
        datos = cur.fetchall()
        print("Datos Encontrados.")

        email_from = "mendietadaniel1994@gmail.com"
        passw = 'xxxxxxxx'
        email_to = "mendietadaniel1994@gmail.com"
        title = "Buenas Noticias"

        df = pd.DataFrame(datos, columns=["ID", "Site ID", "Title", "Price", "Sold Quantity", "Thumbnail ID", "Fecha y Hora"])

        css_styles = """
        <style> 
            table {
                width: 100%;
                border-collapse: collapse;
            }
            th, td {
                text-align: center;
                border: 1px solid black;
                padding: 8px;
            }
            th {
                background-color: #f2f2f2;
            }
            tr:nth-child(even) {
                background-color: #f2f2f2;
            }
        </style>
        """
        
        body = """<!DOCTYPE html> 
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Productos con más ventas</title>
            {css_styles} 
        </head>
        <body>
            <h2>Estos productos han superado los $10.000.000 en ganancias:</h2>
            <table>
                {table_content} 
            </table>
            <p class="footer">Fecha y Hora: {fecha_hora_actual}</p>
        </body>
        </html>""".format(css_styles=css_styles, table_content=df.to_html(index=False), fecha_hora_actual=fechaHoraActual)

        email = EmailMessage()
        email['From'] = email_from
        email['To'] = email_to
        email['Subject'] = title
        email.add_alternative(body, subtype='html')  

        # Establecer la conexión segura con el servidor SMTP
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(email_from, passw)
            smtp.send_message(email)
        
        print("¡Correo electrónico enviado!")

    except Exception as e:
        print("Error al realizar la consulta o enviar el correo electrónico", str(e))
        
    finally:
        conn.close()
        print("Conexión Terminada.")

# Airflow
# Argumentos para mi DAG
default_args = {
    'owner': 'DanielMendieta',
    'depends_on_past': True,
    'email': ['mendietadaniel1994@gmail.com'],
    'email_on_failure': True,
    'start_date': datetime(2023,7,30),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

BC_dag = DAG(
    dag_id='MercadoLibre',
    default_args=default_args,
    description='Ventas',
    schedule_interval="@daily",
    tags=["Ventas", "Microondas"],
    catchup=False
)

#Tasks Airflow
task_1 = PythonOperator(
    task_id='Extraer_datos_mercado_libre',
    python_callable=extraer_datos_mercado_libre,
    provide_context=True,
    dag=BC_dag,
)

task_2 = PythonOperator(
    task_id='Crear_conexion_insert',
    python_callable=crear_conexion_insert,
    op_args=[task_1.output],  # Obtener el resultado de la tarea anterior
    dag=BC_dag,
)

task_3 = PythonOperator(
    task_id='ConsultaSql_email',
    python_callable=consulta_email,
    dag=BC_dag,
)

task_1 >> task_2 >> task_3
