import requests
import boto3
import uuid

def lambda_handler(event, context):

    # API real que devuelve los datos directamente en JSON
    api_url = "https://ultimosismo.igp.gob.pe/api/sismos/reportados?year=2025"

    response = requests.get(api_url, timeout=15)

    if response.status_code != 200:
        return {
            "statusCode": response.status_code,
            "body": "No se pudo acceder al API del IGP"
        }

    data = response.json()

    # La estructura real está en data["data"]
    sismos = data.get("data", [])

    # Tomamos los últimos 10
    ultimos_10 = sismos[:10]

    # Formato final para DynamoDB
    cleaned = []
    for s in ultimos_10:
        cleaned.append({
            "id": str(uuid.uuid4()),
            "reporte": s.get("reporte_sismico", ""),
            "referencia": s.get("referencia", ""),
            "fecha_hora": s.get("fecha_local", ""),
            "magnitud": str(s.get("magnitud", "")),
            "enlace": "https://ultimosismo.igp.gob.pe/evento/" + s.get("codigo", "")
        })

    # Guardar en DynamoDB
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("TablaSismosIGP")

    # Limpiar antes de insertar
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get("Items", []):
            batch.delete_item(Key={"id": item["id"]})

    # Insertar nuevos
    with table.batch_writer() as batch:
        for item in cleaned:
            batch.put_item(Item=item)

    return {
        "statusCode": 200,
        "body": cleaned
    }
