# Filename: gestion_adopciones_lambda.py
# Author: Gerardo Cataño
# Created: 2024-07-16
# Description: Módulo Back-end de gestión de adopciones para despliegue en servicio AWS Lambda

import json
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal
from boto3.dynamodb.conditions import Key

#Inicializa rutas
solicitudes_path = '/api/solicitudes'
mascotas_path = '/api/mascotas'
usuarios_path = '/api/usuarios'
centros_path = '/api/centros'
hogares_path = '/api/hogares'

#Inicializa cliente DynamoDB y tablas
dynamodb = boto3.resource('dynamodb', region_name='us-east-2')  #interfaz de recursos: actualizar region
solicitudes_table = dynamodb.Table('SolicitudAdopcion')
mascotas_table = dynamodb.Table('MascotaAdopcion')
usuarios_table = dynamodb.Table('UsuarioAdoptante')
centros_table = dynamodb.Table('CentroAdopcion')
hogares_table = dynamodb.Table('HogarAdoptante')


def lambda_handler(event, context):
    print('***** Debug: event: ', event)
    print('***** Debug: method=', event['requestContext']['http']['method'], ' path=', event['requestContext']['http']['path'])    
    response = None
   
    try:
        http_method = event['requestContext']['http']['method']
        path = event['requestContext']['http']['path']

        #Lógica basada en rutas y posteriormente en métodos
        if path == solicitudes_path:
        
            if http_method == 'GET':
                #Logica para saber si solicitudId existe en queryStringParameters de evento
                isSolicitudIdPresent = False
                for i in event:
                    if 'queryStringParameters' in i:
                        solicitud_id = event['queryStringParameters']['solicitudId']
                        response = consulta_item(solicitudes_table, 'solicitudId', solicitud_id)
                        isSolicitudIdPresent = True
                        break
                #not in for i:
                if isSolicitudIdPresent != True:
                    response = consulta_items(solicitudes_table)
    
            elif http_method == 'POST':
                response = alta_item(solicitudes_table, json.loads(event['body']))
                
            elif http_method == 'PUT':
                body = json.loads(event['body'])
                solicitud_id = event['queryStringParameters']['solicitudId']
                response = actualiza_item(solicitudes_table, 'solicitudId', solicitud_id, body['atributo'], body['valor'])
            
            elif http_method == 'DELETE':
                solicitud_id = event['queryStringParameters']['solicitudId']
                response = baja_item(solicitudes_table, 'solicitudId', solicitud_id)
                
            else:
                return build_response(405, '405: Metodo no permitido')


        elif path == mascotas_path:
        
            if http_method == 'GET':
                #Logica para saber si mascotaId existe en queryStringParameters de evento
                isMascotaIdPresent = False
                for i in event:
                    if 'queryStringParameters' in i:
                        mascota_id = event['queryStringParameters']['mascotaId']
                        response = consulta_item(mascotas_table, 'mascotaId', mascota_id)
                        isMascotaIdPresent = True
                        break
                #not in for i:
                if isMascotaIdPresent != True:
                    response = consulta_items(mascotas_table)
    
            elif http_method == 'POST':
                response = alta_item(mascotas_table, json.loads(event['body']))

            elif http_method == 'PUT':
                body = json.loads(event['body'])
                mascota_id = event['queryStringParameters']['mascotaId']
                response = actualiza_item(mascotas_table, 'mascotaId', mascota_id, body['atributo'], body['valor'])

            elif http_method == 'DELETE':
                mascota_id = event['queryStringParameters']['mascotaId']
                response = baja_item(mascotas_table, 'mascotaId', mascota_id)
                
            else:
                return build_response(405, '405: Metodo no permitido')


        elif path == usuarios_path:
        
            if http_method == 'GET':
                #Logica para saber si usuarioId existe en queryStringParameters de evento
                isUsuarioIdPresent = False
                for i in event:
                    if 'queryStringParameters' in i:
                        usuario_id = event['queryStringParameters']['usuarioId']
                        response = consulta_item(usuarios_table, 'usuarioId', usuario_id)
                        isUsuarioIdPresent = True
                        break
                #not in for i:
                if isUsuarioIdPresent != True:
                    response = consulta_items(usuarios_table)
    
            elif http_method == 'POST':
                response = alta_item(usuarios_table, json.loads(event['body']))

            elif http_method == 'PUT':
                body = json.loads(event['body'])
                usuario_id = event['queryStringParameters']['usuarioId']
                response = actualiza_item(usuarios_table, 'usuarioId', usuario_id, body['atributo'], body['valor'])

            elif http_method == 'DELETE':
                usuario_id = event['queryStringParameters']['usuarioId']
                response = baja_item(usuarios_table, 'usuarioId', usuario_id)
                
            else:
                return build_response(405, '405: Metodo no permitido')


        elif path == centros_path:
        
            if http_method == 'GET':
                #Logica para saber si centroId existe en queryStringParameters de evento
                isCentroIdPresent = False
                for i in event:
                    if 'queryStringParameters' in i:
                        centro_id = event['queryStringParameters']['centroId']
                        response = consulta_item(centros_table, 'centroId', centro_id)
                        isCentroIdPresent = True
                        break
                #not in for i:
                if isCentroIdPresent != True:
                    response = consulta_items(centros_table)
    
            elif http_method == 'POST':
                response = alta_item(centros_table, json.loads(event['body']))

            elif http_method == 'PUT':
                body = json.loads(event['body'])
                centro_id = event['queryStringParameters']['centroId']
                response = actualiza_item(centros_table, 'centroId', centro_id, body['atributo'], body['valor'])

            elif http_method == 'DELETE':
                centro_id = event['queryStringParameters']['centroId']
                response = baja_item(centros_table, 'centroId', centro_id)
                
            else:
                return build_response(405, '405: Metodo no permitido')


        elif path == hogares_path:
        
            if http_method == 'GET':
                #Logica para saber si hogarId existe en queryStringParameters de evento
                isHogarIdPresent = False
                for i in event:
                    if 'queryStringParameters' in i:
                        hogar_id = event['queryStringParameters']['hogarId']
                        response = consulta_item(hogares_table, 'hogarId', hogar_id)
                        isHogarIdPresent = True
                        break
                #not in for i:
                if isHogarIdPresent != True:
                    response = consulta_items(hogares_table)
    
            elif http_method == 'POST':
                response = alta_item(hogares_table, json.loads(event['body']))

            elif http_method == 'PUT':
                body = json.loads(event['body'])
                hogar_id = event['queryStringParameters']['hogarId']
                response = actualiza_item(hogares_table, 'hogarId', hogar_id, body['atributo'], body['valor'])

            elif http_method == 'DELETE':
                hogar_id = event['queryStringParameters']['hogarId']
                response = baja_item(hogares_table, 'hogarId', hogar_id)
                
            else:
                return build_response(405, '405: Metodo no permitido')

  
        else:
            response = build_response(404, '404: Recurso no encontrado')        #####not reachable

    except Exception as e:
        print('Error:', e)
        response = build_response(400, 'Error en peticion HTTP')
   
    return response


def consulta_item(dynamodb_table, item_key, item_id):
    try:
        response = dynamodb_table.get_item(Key={item_key: item_id})        
        #Logica para saber si Item existe en response
        isItemPresent = False
        for i in response:
            if 'Item' in i:                
                return build_response(200, response.get('Item'))
                isItemPresent = True
                break
        #not in for i:
        if isItemPresent != True:
            return build_response(404, '404: Item no encontrado')
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])


def consulta_items(dynamodb_table):
    try:
        scan_params = {
            'TableName': dynamodb_table.name
        }
        return build_response(200, scan_dynamo_records(dynamodb_table, scan_params, []))
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])

def scan_dynamo_records(dynamodb_table, scan_params, item_array):
    response = dynamodb_table.scan(**scan_params)
    item_array.extend(response.get('Items', []))   
    if 'LastEvaluatedKey' in response:
        scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
        return scan_dynamo_records(scan_params, item_array)
    else:
        return {'items': item_array}


def alta_item(dynamodb_table, request_body):
    try:
        dynamodb_table.put_item(Item=request_body)
        body = {
            'Operacion': 'Alta',
            'Mensaje': 'Exitosa',
            'Item': request_body
        }
        return build_response(201, body)
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])


def actualiza_item(dynamodb_table, item_key, item_id, update_key, update_value):
    try:
        response = dynamodb_table.update_item(
            Key={item_key: item_id},
            UpdateExpression=f'SET {update_key} = :value',
            ExpressionAttributeValues={':value': update_value},
            ReturnValues='UPDATED_NEW'
        )
        body = {
            'Operacion': 'Actualizacion',
            'Mensaje': 'Exitosa',
            'UpdatedAttributes': response
        }
        return build_response(200, body)
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])


def baja_item(dynamodb_table, item_key, item_id):
    try:
        response = dynamodb_table.delete_item(
            Key={item_key: item_id},
            ReturnValues='ALL_OLD'
        )
        body = {
            'Operacion': 'Baja',
            'Mensaje': 'Exitosa',
            'Item': response
        }
        return build_response(200, body)
    except ClientError as e:
        print('Error:', e)
        return build_response(400, e.response['Error']['Message'])


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            # Check if it's an int or a float
            if obj % 1 == 0:
                return int(obj)
            else:
                return float(obj)
        # Let the base class default method raise the TypeError
        return super(DecimalEncoder, self).default(obj)

def build_response(status_code, body):
    print('***** Debug: response: status=', status_code, ' body=', body)
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }