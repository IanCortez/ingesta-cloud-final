import json
import urllib3


def lambda_handler(event, context):
    http = urllib3.PoolManager()
    print(event)
    for record in event['Records']:
        print("ingreso aqui")
        if record['eventName'] in ['INSERT', 'MODIFY']:
            print("evento INSERT o MODIFY")
            new_image = record['dynamodb']['NewImage']
            
            tenant_id = new_image["tenant_id"]['S']
            elastic_port = {
                "inkafarma": 9201,
                "mifarma": 9202
            }[tenant_id]

            producto_id = new_image['codigo']['S']
            nombre = new_image['nombre']['S']
            precio = float(new_image['precio']['N'])

            doc = {
                "codigo": producto_id,
                "nombre": nombre,
                "precio": precio
            }
            print(f"datos de envio: {doc}")

            es_url = f"http://54.197.98.134:{elastic_port}/productos/_doc/{producto_id}"
            print(f"URL: {es_url}")

            res = http.request(
                "PUT",
                es_url,
                body=json.dumps(doc).encode("utf-8"),
                headers={"Content-Type": "application/json"}
            )
            print(f"Indexado producto {producto_id}: {res.status} {res.data}")

        elif record['eventName'] == 'REMOVE':
            print("evento REMOVE")
            old_image = record['dynamodb']['OldImage']

            tenant_id = new_image["tenant_id"]['S']
            elastic_port = {
                "inkafarma": 9201,
                "mifarma": 9202
            }[tenant_id]

            producto_id = old_image['codigo']['S']
            
            es_url = f"http://54.197.98.134:{elastic_port}/productos/_doc/{producto_id}"
            print(f"URL: {es_url}")

            res = http.request(
                "DELETE",
                es_url
            )
            print(f"Eliminado producto {producto_id}: {res.status} {res.data}")

    return {
        'statusCode': 200,
        'body': json.dumps('Procesado correctamente.')
    }