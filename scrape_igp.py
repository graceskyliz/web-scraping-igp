import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):

    # URL oficial de últimos sismos del IGP
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        return {
            "statusCode": response.status_code,
            "body": "Error al acceder a la página del IGP"
        }

    soup = BeautifulSoup(response.content, "html.parser")

    # La tabla del IGP usa <table class="table table-striped">
    table = soup.find("table")
    if not table:
        return {
            "statusCode": 404,
            "body": "No se encontró la tabla en el IGP"
        }

    # Obtener encabezados
    headers = [h.text.strip() for h in table.find_all("th")]

    # Obtener filas
    rows = []
    for row in table.find_all("tr")[1:]:  # saltar encabezado
        cols = [c.text.strip() for c in row.find_all("td")]
        if len(cols) == 0:
            continue
        rows.append({headers[i]: cols[i] for i in range(len(cols))})

    # Quedarse solo con los 10 últimos sismos
    rows = rows[:10]

    # Conectar con DynamoDB
    dynamodb = boto3.resource("dynamodb")
    table_db = dynamodb.Table("TablaSismosIGP")

    # Limpiar tabla
    scan = table_db.scan()
    with table_db.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"id": item["id"]})

    # Insertar datos nuevos
    for r in rows:
        r["id"] = str(uuid.uuid4())
        table_db.put_item(Item=r)

    return {
        "statusCode": 200,
        "body": rows
    }
