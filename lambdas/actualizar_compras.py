import json
import boto3


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    print(event)
    for record in event['Records']:
        print("ingreso aqui")
        if record['eventName'] in ['INSERT', 'MODIFY']:
            print("evento INSERT")
            new_image = record['dynamodb']['NewImage']

            compra_id = new_image['codigo_compra']['S']

            # lista de productos
            products = new_image['productos']['L'] 

            for t in products:
                product = t["M"]
                codigo = product["codigo"]["S"]
                precio = float(product["precio"]["N"])
                subtotal = float(product["subtotal"]["N"])
                cantidad = float(product["cantidad"]["N"])
                nombre = product["nombre"]["S"]
                row = {
                    "codigo": codigo,
                    "compra_id": compra_id,
                    "precio": precio,
                    "subtotal": subtotal,
                    "cantidad": cantidad,
                    "nombre": nombre
                }

                s3.put_object(
                    Bucket="dev-actualizar-compras",
                    Key=f"{codigo}.json",
                    Body=json.dumps(row),
                    ContentType='application/json'
                )

                print(f"Subido archivo JSON a bucket")

    return {
        'statusCode': 200,
        'body': json.dumps('Procesado correctamente.')
    }