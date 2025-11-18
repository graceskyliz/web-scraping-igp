import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):

    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    # Request simple, sin JS
    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        return {
            "statusCode": response.status_code,
            "body": "Error al acceder al sitio del IGP"
        }

    soup = BeautifulSoup(response.text, "html.parser")

    # Extraer la tabla real
    table = soup.find("table", class_="table")
    if not table:
        return {
            "statusCode": 404,
            "body": "Tabla no encontrada en el HTML del IGP"
        }

    # Columnas HTML:
    # 1. Reporte sísmico
    # 2. Referencia
    # 3. Fecha y hora
    # 4. Magnitud
    # 5. Descargas (link)
    rows_data = []

    tbody = table.find("tbody")
    rows = tbody.find_all("tr")

    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        reporte = tds[0].get_text(strip=True).replace("\n", " ")
        referencia = tds[1].get_text(strip=True)
        fecha_hora = tds[2].get_text(strip=True)
        magnitud = tds[3].get_text(strip=True)

        # link de descarga del reporte sísmico
        descarga_tag = tds[4].find("a")
        link_descarga = "https://ultimosismo.igp.gob.pe" + descarga_tag["href"] if descarga_tag else None

        rows_data.append({
            "reporte": reporte,
            "referencia": referencia,
            "fecha_hora_local": fecha_hora,
            "magnitud": magnitud,
            "reporte_url": link_descarga
        })

    # Tomar solo los 10 últimos
    rows_data = rows_data[:10]

    # Guardar en DynamoDB
    dynamodb = boto3.resource("dynamodb")
    table_db = dynamodb.Table("TablaSismosIGP")

    # Limpiar tabla
    scan = table_db.scan()
    with table_db.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"id": item["id"]})

    # Insertar data
    for item in rows_data:
        item["id"] = str(uuid.uuid4())
        table_db.put_item(Item=item)

    return {
        "statusCode": 200,
        "body": rows_data
    }
