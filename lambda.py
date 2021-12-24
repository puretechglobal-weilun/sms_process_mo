# 700-810 ms
import json
import uuid
import boto3

def handler(event,context):

    function_class = __import__("function_class")
    internal_status = 200
    internal_debug = {}
    request_json = {}
    global_datetime = getattr(function_class, "UTC0_datetime")()
    global_date = getattr(function_class, "UTC0_date")()
    unique_id = str(uuid.uuid4())
    record = event.get("Records").pop()
    message = record.get("body")
    event = json.loads(message)
    try:
        event["country"], event["gateway"], event["operator"], event["shortcode"], event["keyword"]
    except KeyError:
        internal_status = "M201"
        internal_debug = "Missing the most important parameter"+"\n"
    else:
        for key, value in event.items():
            request_json[key] = value.lower()
            
        request_json["function"] = "insert_mo"
        request_json["global_datetime"] = global_datetime
        mo_var = getattr(function_class, "invoke_function")(request_json)
        internal_debug['insert_mo'] = mo_var
        mo_var = list(mo_var.values())[0]
        if mo_var["mo_type"] == "stopall" or mo_var["mo_type"] == "unsub all":
            pass
        else:
            keyword_detail = getattr(function_class, "search_keyword")(request_json["country"], request_json["gateway"], request_json["operator"], request_json["shortcode"], request_json["keyword"])
            internal_debug['keyword_detail'] = keyword_detail = keyword_detail
            if keyword_detail["product"] == "product not found":
                internal_status = keyword_detail["code"]
            else:
                for key, value in keyword_detail.items():
                    mo_var[key] = value.lower()
                    
        if internal_status == 200:
            if mo_var["mo_type"] == "sub" or mo_var["mo_type"] == "iod":
                # filter_blacklist_msisdn and filter_multiple_subscription and gateway_whitelist_duplicate_subscriber is pending team discussion
                new_subscriber     = "yes"
                message_key        = "welcome"
                subscriber_data = {
                    "rid"               : "",
                    "operator"          : mo_var.get("operator", ""),
                    "shortcode"         : mo_var.get("shortcode", ""),
                    "keyword"           : mo_var.get("keyword", ""),
                    "msisdn"            : mo_var.get("msisdn", ""),
                    "mo_id"             : mo_var.get("mo_id", ""),
                    "subscribe_time"    : mo_var.get("date_time", ""),
                    "unsubscribe_time"  : "0000-00-00 00:00:00",
                    "sub_status"        : "1",
                    "investor_campaign" : mo_var.get("investor_campaign", "")
                }
                request_json["function"] = "process_subscriber_add_data"
                subscriber_add_data = getattr(function_class, "invoke_function")(request_json)
                internal_debug['process_subscriber_add_data'] = subscriber_add_data
                subscriber_add_data = list(subscriber_add_data.values())[0]
                
                for key, value in subscriber_add_data.items():
                    subscriber_data[key] = str(value)
                    
                insert_subscriber_result = getattr(function_class, "insert_subscriber")(subscriber_data)
                internal_debug['insert_subscriber'] = insert_subscriber_result
                
                if keyword_detail["investor_campaign"] == "880000":
                    insert_cps_result = getattr(function_class, "insert_cps")(mo_var)
                    internal_debug['insert_cps'] = insert_cps_result           

                request_json["subscriber_id"]  = insert_subscriber_result["rid"]
                request_json["function"]       = "process_send_sms"
                request_json["message_key"]    = message_key
                request_json["new_subscriber"] = new_subscriber
                send_sms_result = getattr(function_class, "invoke_function")(request_json)
                internal_debug['process_send_sms'] = send_sms_result
                
                request_json["function"]       = "process_send_content"
                send_content_result = getattr(function_class, "invoke_function")(request_json)
                internal_debug['process_send_content'] = send_content_result
                
            elif mo_var["mo_type"] == "stop" or mo_var["mo_type"] == "unsub":
                message_key = "quit_message"
                list_subscriber = getattr(function_class, "search_subscriber")(mo_var["subscriber_id"])
                if list_subscriber:
                    internal_debug['search_subscriber'] = list_subscriber
                    for per_subscriber in list_subscriber:
                        per_subscriber["unsubscribe_time"] = global_datetime
                        unsub_subscriber = getattr(function_class, "unsub_subscriber")(per_subscriber)
                        internal_debug['unsub_subscriber'] = unsub_subscriber
                        if per_subscriber["investor_campaign"] == "880000":
                            insert_cps_result = getattr(function_class, "insert_cps")(per_subscriber)
                            internal_debug['insert_cps'] = insert_cps_result 

                else:
                    internal_status = "M204"
                    internal_debug['search_subscriber'] = "subscriber no found"
                    
                request_json["function"]       = "process_send_sms"
                request_json["message_key"]    = message_key
                send_sms_result = getattr(function_class, "invoke_function")(request_json)
                internal_debug['process_send_sms'] = send_sms_result
                
            elif mo_var["mo_type"] == "stopall" or mo_var["mo_type"] == "unsub all":
                message_key = "stop_all_message"
                list_subscriber = getattr(function_class, "search_subscriber")(mo_var["subscriber_id"], search_by = "stopall")
                
                if list_subscriber:
                    internal_debug['search_subscriber'] = list_subscriber
                    for per_subscriber in list_subscriber:
                        per_subscriber["unsubscribe_time"] = global_datetime
                        unsub_subscriber = getattr(function_class, "unsub_subscriber")(per_subscriber)
                        internal_debug['unsub_subscriber'] = unsub_subscriber
                        if per_subscriber["investor_campaign"] == "880000":
                            insert_cps_result = getattr(function_class, "insert_cps")(per_subscriber)
                            internal_debug['insert_cps'] = insert_cps_result 
                else:
                    internal_status = "M204"
                    internal_debug['search_subscriber'] = "subscriber no found"
                    message_key = "non_subscriber"
                    
                request_json["function"]       = "process_send_sms"
                request_json["message_key"]    = message_key
                send_sms_result = getattr(function_class, "invoke_function")(request_json)
                internal_debug['process_send_sms'] = send_sms_result
            else:
                internal_status = "M203"
                internal_debug['unknown_mo'] = "unknown mo type"
                
    insert_log = getattr(function_class, "insert_log")(request_json, internal_debug)
    s3 = boto3.resource("s3")
    object = s3.Object("request-test-bucket", "mo/"+request_json["gateway"]+"/"+request_json["country"]+"/"+global_date+"/"+unique_id+".json")
    object.put(Body=str(insert_log))
    print("check debug using: "+unique_id)
    return {
        "status"    :   internal_status,
        "code"      :   internal_debug
    }
