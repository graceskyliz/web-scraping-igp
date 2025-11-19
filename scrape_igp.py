from playwright.sync_api import sync_playwright
import boto3
import uuid

def lambda_handler(event, context):
    with sync_playwright() as p:
        browser = p.chromium.launch(args=["--no-sandbox"], headless=True)
        page = browser.new_page()

        page.goto("https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados", wait_until="networkidle")

        # extract table rows after Angular renders
        rows = page.query_selector_all("table tbody tr")

        extracted = []

        for r in rows[:10]:
            cols = r.query_selector_all("td")
            extracted.append({
                "id": str(uuid.uuid4()),
                "reporte": cols[0].inner_text().strip(),
                "referencia": cols[1].inner_text().strip(),
                "fecha_hora": cols[2].inner_text().strip(),
                "magnitud": cols[3].inner_text().strip(),
                "link": cols[4].query_selector("a").get_attribute("href")
            })

        browser.close()

    # Save to DynamoDB
    dynamo = boto3.resource("dynamodb")
    table = dynamo.Table("TablaSismosIGP")

    with table.batch_writer() as batch:
        for item in extracted:
            batch.put_item(Item=item)

    return {"statusCode": 200, "body": extracted}
