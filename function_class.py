import boto3
import json
from datetime import datetime
from boto3.dynamodb.conditions import Key
import uuid

lambda_client = boto3.client("lambda")
dynamodb = boto3.resource("dynamodb")
sqs = boto3.resource("sqs")

def invoke_function(params):
    gateway = params["gateway"]
    response = lambda_client.invoke(
        FunctionName =  "arn:aws:lambda:us-east-1:344055016255:function:"+'sms_'+gateway,
        InvocationType = "RequestResponse",
        Payload = json.dumps(params)
    )
    return json.load(response["Payload"])
    
def UTC0_datetime():
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")

def UTC0_date():
    now = datetime.now()
    return now.strftime("%Y-%m-%d")

def search_keyword(country,gateway,operator,shortcode,keyword):
    product = country+"_"+gateway+"_"+operator+"_"+shortcode+"_"+keyword
    table = dynamodb.Table("keyword")
    response = table.get_item(
        Key={
            "country": country,
            "product": product
        }
    )
    if "Item" in response:
        return response["Item"]
    else:
        return {
            "product"    :   "product not found",
            "code"       :   "M202"
        }

def search_subscriber(rid, search_by = "stop"):
    rid_list    = rid.split("_")
    country     = rid_list[0]
    gateway     = rid_list[1]
    operator    = rid_list[2]
    shortcode   = rid_list[3]
    keyword     = rid_list[4]
    msisdn      = rid_list[5]
    stop_rid = country+"_"+gateway+"_"+operator+"_"+shortcode+"_"+keyword+"_"+msisdn
    stop_all_rid = country+"_"+gateway+"_"+operator+"_"+shortcode
    rid = stop_all_rid if search_by == "stopall" else stop_rid
    table = dynamodb.Table("subscriber_"+gateway)
    response = table.query(
        IndexName="msisdn-rid-index",
        KeyConditionExpression= Key("msisdn").eq(msisdn) & Key("rid").begins_with(rid)
    )
    active_user = []
    if "Items" in response:
        list_subscriber = response["Items"]
        for key in list_subscriber:
            if key["sub_status"] == "S101":
                active_user.append(key)
        return active_user
    else:
        return "subscriber no found"

def insert_subscriber(function_json):
    dynamoDB_status = ""
    rid = function_json["country"]+"_"+function_json["gateway"]+"_"+function_json["operator"]+"_"+function_json["shortcode"]+"_"+function_json["keyword"]+"_"+function_json["msisdn"]+"_"+function_json["mo_id"]+"_subscriber"
    function_json["rid"] = rid
    table = dynamodb.Table("subscriber_"+function_json["gateway"])
    dynamoDB_status = table.put_item(Item=function_json)
    dynamoDB_status = dynamoDB_status["ResponseMetadata"]["HTTPStatusCode"]
    
    if dynamoDB_status == 200:
        return function_json
    else:
        return "DynamoDB got problem"

def insert_cps(function_json):
    message_body =  json.dumps(function_json)
    queue = sqs.get_queue_by_name(QueueName="cps")
    response = queue.send_message(MessageBody=message_body)
    return response

def unsub_subscriber(function_json):
    dynamoDB_status = ""
    rid = function_json["rid"]
    subscribe_time = function_json["subscribe_time"]
    unsubscribe_time = function_json["unsubscribe_time"]
    table = dynamodb.Table("subscriber_"+function_json["gateway"])
    dynamoDB_status = table.update_item(
        Key={
            "rid": str(rid),
            "subscribe_time": str(subscribe_time)
        },
        UpdateExpression="SET sub_status = :val1, unsubscribe_time = :val2",
        ExpressionAttributeValues={
            ":val1": "S102",
            ":val2": unsubscribe_time
        }
    )
    dynamoDB_status = dynamoDB_status["ResponseMetadata"]["HTTPStatusCode"]
    if dynamoDB_status == 200:
        return "Unsub done: "+rid
    else:
        return "DynamoDB got problem"
        
def insert_log(request_json, internal_debug):
    debug = {}
    debug["request_params"] = request_json
    debug["internal_debug"] = internal_debug
    debug =  json.dumps(debug)
    return debug