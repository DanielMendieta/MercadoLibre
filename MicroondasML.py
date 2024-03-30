import requests
import pandas as pd
from datetime import datetime, timedelta
import smtplib
from email.message import EmailMessage
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Configuración de fecha y hora actual
fecha_hora_actual = datetime.now()
fechaHoraActual = fecha_hora_actual.strftime("%d/%m/%Y %H:%M")

def extraer_datos_api():
    try:
        url = "https://api.mercadolibre.com/sites/MLA/search?category=MLA1577#json"  # URL de la API de MercadoLibre
        response = requests.get(url)  # Realizar solicitud GET a la API y almacenar la respuesta
        data = response.json()  # Convertir la respuesta a formato JSON
        results = data.get("results", [])  # Extraer la lista de resultados de la respuesta JSON
        df = pd.DataFrame(results)  # Crear un DataFrame de Pandas a partir de los resultados
        columnas_seleccionadas = ["id","title", "price", "available_quantity", "permalink", "condition"]  # Seleccionar columnas relevantes del DataFrame
        df = df[columnas_seleccionadas]
        df["fecha"] = fechaHoraActual  # Agregar una columna de fecha al DataFrame
        datos = df  # Almacenar los datos en la variable 'datos'
    except Exception as e:
        print("No se pudo extraer los datos.", str(e))  # Manejar errores e imprimir un mensaje de error
    return datos  # Devolver el DataFrame resultante con los datos extraídos
    

def crear_tabla(): #Funcion para crear la tabla en nuestra base de datos PostgreSQL
    try:
        sql = """
          create table if not exists microondas (
                         id varchar(30),
                         Nombre varchar (255),
                         Precio numeric (10,2),
                         Cantidad_Disponible Integer,   
	                       Link varchar(300),			
                         Estado varchar(10),
                         Fecha timestamp(300),
                         primary key (id)
                )      
                """
    except Exception as e:
        print("No fue posible crear la tabla.", str(e))  
    return sql

def insertar_datos():  # Función para insertar datos en la tabla de la base de datos
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default_daniel')  # Conexión a la base de datos PostgreSQL
        connection = postgres_hook.get_conn()  # Obtener la conexión
        cursor = connection.cursor()  # Crear un cursor para ejecutar consultas SQL
        df = extraer_datos_api()  # Obtener los datos a insertar desde la API
        data = df.values.tolist()  # Convertir los datos a una lista de listas
        insert_query = """
            INSERT INTO microondas (id, nombre, precio, cantidad_disponible, link, estado, fecha)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """  # Consulta SQL para insertar datos en la tabla
        values = (data)  # Valores que vamos a insertar
        cursor.executemany(insert_query, values)  # Ejecutar la consulta SQL con los valores
        connection.commit()  # Confirmar la transacción
    except Exception as e:
        print("No fue posible insertar los datos.", str(e))  # Manejar errores e imprimir un mensaje de error
        
def E_mail():  # Función para enviar correo electrónico
    try:
        # Establecer conexión con la base de datos PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default_daniel')
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()   
        
        # Consulta SQL para obtener datos de microondas de la marca 'Bgh'
        sql = """select nombre, precio, cantidad_disponible, link, estado from microondas where nombre like '%Bgh%';"""     
        cursor.execute(sql)  # Ejecutar la consulta SQL
        datos = cursor.fetchall()  # Obtener los resultados de la consulta
        connection.commit()  # Confirmar la transacción
        
        # Configuración para enviar el correo electrónico
        email_from = "mendietadaniel1994@gmail.com"
        passw = 'XXXXXX'
        email_to = "mendietadaniel1994@gmail.com"
        title = "Buenas Noticias"
        
        # Crear un DataFrame con los datos obtenidos de la base de datos
        df = pd.DataFrame(datos, columns=["Nombre", "Precio", "Cantidad_Disponible", "Link", "Estado"])
        
        # Estilos CSS para el correo electrónico
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
        
        # Cuerpo del correo electrónico en formato HTML
        body = """<!DOCTYPE html> 
            <html>
            <head>
                <meta charset="UTF-8">
                <title>La marca que esperabas</title>
                {css_styles} 
            </head>
            <body>
                <h2>Finalmente ingresaron Microondas Bgh:</h2>
                <table>
                    {table_content} 
                </table>
                <p class="footer">Fecha y Hora: {fecha_hora_actual}</p>
            </body>
            </html>""".format(css_styles=css_styles, table_content=df.to_html(index=False), fecha_hora_actual=fechaHoraActual)
        
        # Configuración del correo electrónico
        email = EmailMessage()
        email['From'] = email_from
        email['To'] = email_to
        email['Subject'] = title
        email.add_alternative(body, subtype='html')  # Adjuntar el cuerpo del correo en formato HTML
        
        # Establecer conexión segura con el servidor SMTP y enviar el correo
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(email_from, passw)
            smtp.send_message(email)
        
        print("¡Correo electrónico enviado!")
            
    except Exception as e:
        print("No fue posible enviar correo de E-mail.", str(e))  # Manejar errores e imprimir un mensaje de error

    

 
                ############### AIRFLOW ###############
                
                
# Argumentos para mi DAG
# Definición de los argumentos por defecto para el DAG
default_args = {
    'owner': 'DanielMendieta',  # Propietario del DAG
    'start_date': datetime(2024, 3, 29),  # Fecha de inicio del DAG
    'retries': 1,  # Número de reintentos en caso de fallo
    'retry_delay': timedelta(seconds=5),  # Retardo entre reintentos
}

# Creación del DAG
BC_dag = DAG(
    dag_id='MercadoLibre',  # Identificador del DAG
    default_args=default_args,  # Argumentos por defecto
    description='Proyecto MercadoLibre',  # Descripción del DAG
    catchup=False,  # No ejecutar tareas para fechas pasadas
)


#Task:
# Definición de las tareas
task_1 = PostgresOperator(
    task_id='creacion_de_tabla',  # Identificador de la tarea
    postgres_conn_id='postgres_default_daniel',  # ID de la conexión a PostgreSQL
    sql=crear_tabla(),  # Consulta SQL a ejecutar
    dag=BC_dag,  # DAG al que pertenece la tarea
)

task_2 = PythonOperator(
    task_id='Extraer_info',  # Identificador de la tarea
    python_callable=extraer_datos_api,  # Función de Python a ejecutar
    provide_context=True,  # Proporcionar contexto a la función
    dag=BC_dag,  # DAG al que pertenece la tarea
)

task_3 = PythonOperator(
    task_id='Insertar_Info',  # Identificador de la tarea
    python_callable=insertar_datos,  # Función de Python a ejecutar
    provide_context=True,  # Proporcionar contexto a la función
    dag=BC_dag,  # DAG al que pertenece la tarea
)

task_4 = PythonOperator(
    task_id='Correo_Email',  # Identificador de la tarea
    python_callable=E_mail,  # Función de Python a ejecutar
    provide_context=True,  # Proporcionar contexto a la función
    dag=BC_dag,  # DAG al que pertenece la tarea
)

# Definición de las dependencias entre tareas
task_1 >> task_2 >> task_3 >> task_4
