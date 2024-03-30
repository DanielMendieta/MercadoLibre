# MercadoLibre AR
Este flujo de trabajo automatizado utiliza Apache Airflow para extraer datos de la API de MercadoLibre, almacenarlos en una base de datos PostgreSQL, realizar consultas en la base de datos para obtener información específica y enviar notificaciones por correo electrónico basadas en los datos extraídos.

Funcionalidades
Extracción de datos de la API de MercadoLibre: Utiliza la función extraer_datos_api para obtener datos de productos de la categoría MLA1577 de la API de MercadoLibre.

Creación de la tabla en la base de datos: La función crear_tabla crea una tabla llamada microondas en la base de datos PostgreSQL para almacenar los datos extraídos.

Inserción de datos en la base de datos: La función insertar_datos inserta los datos extraídos de la API de MercadoLibre en la tabla microondas de la base de datos PostgreSQL.

Envío de correo electrónico: La función E_mail consulta la base de datos para obtener información sobre los productos de microondas de la marca "Bgh" y envía un correo electrónico HTML con esta información.

Configuración
Para ejecutar este proyecto, sigue estos pasos:

Clona este repositorio en tu máquina local.
Instala las dependencias necesarias utilizando pip install -r requirements.txt.
Configura una base de datos PostgreSQL y actualiza la conexión en el código.
Configura una cuenta de correo electrónico y actualiza las credenciales en el código.
Ejecuta el script principal para iniciar el flujo de trabajo.

Este proyecto utiliza las siguientes tecnologías:
Apache Airflow,
Dbeaver,
Docker,
Postgresql,
Python (datetime,email.message,pandas,Requests,smtplib),
Visual studio code
