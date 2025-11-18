import requests
import boto3
import uuid

def lambda_handler(event, context):

    url = "https://ultimosismo.igp.gob.pe/api/sismos/reportados?year=2025"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://ultimosismo.igp.gob.pe/"
    }

    try:
        response = requests.get(url, headers=headers, timeout=20)
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error en conexión: {str(e)}"
        }

    if response.status_code != 200:
        return {
            "statusCode": response.status_code,
            "body": "No se pudo acceder al API del IGP"
        }

    data = response.json()
    sismos = data.get("data", [])

    ultimos_10 = sismos[:10]

    cleaned = []
    for s in ultimos_10:
        cleaned.append({
            "id": str(uuid.uuid4()),
            "reporte": s.get("reporte_sismico"),
            "referencia": s.get("referencia"),
            "fecha_hora": s.get("fecha_local"),
            "magnitud": str(s.get("magnitud")),
            "enlace": "https://ultimosismo.igp.gob.pe/evento/" + s.get("codigo")
        })

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("TablaSismosIGP")

    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"id": item["id"]})

    with table.batch_writer() as batch:
        for item in cleaned:
            batch.put_item(Item=item)

    return {
        "statusCode": 200,
        "body": cleaned
    }
