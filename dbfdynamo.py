import boto3
import time

from pprint import pprint
from botocore.exceptions import ClientError
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from lib.dbfread import DBF


dynamoClient = boto3.client('dynamodb')
dbfTable = DBF('/Volumes/ramdisk/SEBANET.DBF')
executor = ThreadPoolExecutor(max_workers=1000)


############ Sequential
# def putToDynamo(item):
#     try:
#         dynamoClient.put_item(
#             TableName = 'SEBANET',
#             Item = ddbItem
#         )
#         # print(dynamoResponse)
#     except ClientError as ex:
#         print(str(ex))
#     except Exception as ex:
#         print(str(ex))
#
# count = 0
# allThreads = []
# for record in dbfTable:
#     ddbItem = dict()
#     for itemKey in record:
#         if record.get(itemKey) == '':
#             continue
#         ddbItem.update({ str(itemKey).strip(): { 'S': str(record.get(itemKey)).strip() } })
#     # thread = Thread(target=putToDynamo, args=·(ddbItem,))
#     # thread.start()
#     # allThreads.append(thread)
#     # _thread.start_new_thread(putToDynamo,(ddbItem,))
#     executor.submit(putToDynamo,ddbItem)
#     count = count + 1
#     print(count)

############ BATCH
itemTemplate = {
    'PutRequest': {
        'Item': {
        }
    }
}
BATCH = list()
count = 0

def putToDynamoBatch(putBatch):
    try:
        dynamoResponse = dynamoClient.batch_write_item(
            RequestItems={
                'SEBANET': putBatch
            }
        )
    except Exception as ex:
        print("*********")
        print("Batch: " + str(batchCount) + " " + str(ex))
        # print("|||||||||")
        # pprint(putBatch)
        # print("|||||||||")
        print("*********")
        time.sleep(10)
        dynamoResponse = dynamoClient.batch_write_item(
            RequestItems={
                'SEBANET': putBatch
            }
        )


for record in dbfTable:
    ddbItem = itemTemplate.copy()
    for itemKey in record:
        if record.get(itemKey) == '':
            continue
        itemDict = {
            str(itemKey).strip():{
                'S': str(record.get(itemKey)).strip()
            }
        }
        ddbItem['PutRequest']['Item'].update(itemDict)
    BATCH.append(deepcopy(ddbItem))
    if count == 24:
        try:
            executor.submit(putToDynamoBatch,deepcopy(BATCH))
            count = 0
            del BATCH[:]
        except Exception as ex:
            print("=========")
            print("Batch: " + str(batchCount) + " " + str(ex))
            print("=========")
    else:
        count = count + 1
