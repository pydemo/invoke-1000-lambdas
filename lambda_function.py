import time
def lambda_handler(event, context):
    time.sleep(10)
    return {'code':0, 'exec_id':event['exec_id']}
