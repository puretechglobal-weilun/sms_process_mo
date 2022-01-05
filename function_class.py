import boto3
import json
from datetime import datetime, timedelta
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

def search_sub_duplicate_filter(country,gateway,operator,shortcode,keyword,msisdn):
    sub_dup_arr ={}
    rid = country+"_"+gateway+"_"+operator+"_"+shortcode+"_"+keyword+"_"+msisdn
    #check setting table
    table = dynamodb.Table("mo_sub_duplicate_setting")
    response = table.query(
        KeyConditionExpression= Key("rid").eq(country + "_" + gateway) & Key("status").eq("m205")
    )
    #default level is keyword level, no period 
    level = "keyword"
    period = 0
    if ("Items" in response) and (response["Count"] != 0):
        sub_dup_arr = list(response["Items"])[0]
        level = sub_dup_arr["level"]
        period = 0 if not level == "keyword" else sub_dup_arr["period"]
    else:
        sub_dup_arr["response"] = "Sub Duplicate record not found"

    #get all subscriber with the same msisdn
    table = dynamodb.Table("subscriber_"+gateway)
    response = table.query(
        IndexName="msisdn-rid-index",
        KeyConditionExpression= Key("msisdn").eq(msisdn) & Key("rid").begins_with(rid)
    )
    #period got not read and put it as string
    sub_dup_arr["period"] = str(period)
    sub_dup_arr["filter"] = ""
    if "Items" in response:
        for item in response["Items"]:
            if item["sub_status"] == "S101" and item["operator"] == operator and item["shortcode"] == shortcode and item["keyword"] == keyword and level == "keyword":
                timeline = check_period(item["subscribe_time"],period)
                if timeline["within_period"] == True:
                    sub_dup_arr["filter"] = "yes"
            elif item["sub_status"] == "S101" and item["operator"] == operator and item["shortcode"] == shortcode and level == "shortcode":
                timeline = check_period(item["subscribe_time"],period)
                if timeline["within_period"] == True:
                    sub_dup_arr["filter"] = "yes"
            elif item["sub_status"] == "S101" and item["operator"] == operator and level == "operator":
                #check if within the period 
                timeline = check_period(item["subscribe_time"],period)
                if timeline["within_period"] == True:
                    sub_dup_arr["filter"] = "yes"
                    
    if sub_dup_arr["filter"] == "yes":
        sub_dup_arr["response"] = "Sub Duplicate per " +level + " level, Last subsribe date : " + timeline["subscribe_time"].strftime("%Y-%m-%d %H:%M:%S") + " | Period : " + str(timeline["days"])
    return sub_dup_arr

def check_period(subscribe_time,period):
    timeline = {}
    #reformat sring to datetime
    timeline["subscribe_time"] = datetime.strptime(subscribe_time, '%Y-%m-%d %H:%M:%S')
    time_between_subscribe = datetime.now() - timeline["subscribe_time"]
    timeline["days"] = time_between_subscribe.days
    if timeline["days"] <= period:
        timeline["within_period"] = True
    else:
        timeline["within_period"] = False
    return timeline

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
    
def insert_pixel(function_json):
    message_body =  json.dumps(function_json)
    queue = sqs.get_queue_by_name(QueueName="pixel")
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

def search_cps_config(campaign):
    table = dynamodb.Table("cps_campaign_config")
    cps_config = {}
    
    #c503 = active campaign
    response = table.query(
        KeyConditionExpression=Key("rid").eq(str(campaign)) & Key("status").eq("c503")
    )
    if ("Items" in response) and (response["Count"] != 0):
        cps_config = list(response["Items"])[0]
    else:
        cps_config["error"] = "CPS configuration record no found"
    return cps_config