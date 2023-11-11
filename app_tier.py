import boto3
import os
import base64
import re
import time
import ast
import json
import logging
import image_classification
import io
import PIL.Image as Image
from array import array
from botocore.exceptions import ClientError
import zipapp

#global
ip_bucket_name='cse546-project1-inputfiles'
op_bucket_name='cse546-project1-results'
sender_queue_url = 'https://sqs.us-east-1.amazonaws.com/836862805331/sender'
#sender_queue_url = 'https://sqs.us-east-1.amazonaws.com/836862805331/image_sender.fifo'
#receiver_queue_url = 'https://sqs.us-east-1.amazonaws.com/836862805331/receiver.fifo'
receiver_queue_url='https://sqs.us-east-1.amazonaws.com/836862805331/receiver'
s3_client = boto3.client('s3',aws_access_key_id='###########',
                         aws_secret_access_key = '###############',
                         region_name="us-east-1")
sqs = boto3.client('sqs',aws_access_key_id='#################',
                   aws_secret_access_key = '######################',
                   region_name="us-east-1")

def s3_upload(file_name, bucket, object_name):

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    try:
        response = s3_client.upload_file(f'/tmp/{file_name}', bucket, object_name)
        # response = s3_client.upload_fileobj(file_name, bucket, object_name,ExtraArgs={
        #     "ContentType": file_name.content_type
        # }),
        s3_url = "https://%s.s3.amazonaws.com/%s" % ( bucket, object_name)
        print ('successfully uploaded in input s3 bucket @'+s3_url)
    except ClientError as e:
        logging.error(e)
        return False
    return None
    #return s3_url

def s3_put(result,key,bucket):

    obj = s3_client.put_object(Body=result, Bucket=bucket, Key=key)
    print('Successfully loaded result object in s3')
    put_tags_response = s3_client.put_object_tagging(
        Bucket=bucket,
        Key=key,
        Tagging={
            'TagSet': [
                {
                    'Key': 'Image',
                    'Value': key
                },
                {
                    'Key': 'Classification',
                    'Value': result
                },
                {
                    'Key': 'ClassifiedBy',
                    'Value': 'image_classification.py'
                }
            ]
        }
    )


def sqs_send(message,url,ID):


    # Send message to SQS queue
    response = sqs.send_message(
        QueueUrl=url,
        #MessageGroupId=ID,
        DelaySeconds=0,
        MessageAttributes={
            'Title': {
                'DataType': 'String',
                'StringValue': 'Input Image location'
            },
            'Author': {
                'DataType': 'String',
                'StringValue': 'WebServer'
            }
        },
        MessageBody=(json.dumps(message)),
    )
   # print("successfully sent message in receiver sqs")

def sqs_receive(url):

    time.sleep(10)
    no_msg = True
    while(no_msg):
        response = sqs.receive_message(
        QueueUrl=url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=['All'],
        VisibilityTimeout=10,
        WaitTimeSeconds=20
     )
        if len(response.get("Messages",[]))==0:
            time.sleep(5)

        if len(response.get("Messages", []))>0:
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            no_msg=False

        else:
            message= None
            receipt_handle=None
            print("No messages available..polling")
            #time.sleep(3)


    # Delete received message from queue

    if message!=None:
        body = message['Body']
        image_file=json.loads(body)
        image=image_file["image"]
        file_name=image_file["filename"]
        sqs.delete_message(
            QueueUrl=url,
            ReceiptHandle=receipt_handle
        )
        print('Received and deleted message: %s' % body)
        return image,file_name

def convert_byte_to_image(image,filename):
    #image = Image.open(io.BytesIO(image))

    image_data = re.sub('^data:image/.+;base64,', '', image)
    decoded = base64.b64decode(image_data)
    image = Image.open(io.BytesIO(decoded))
    image.save(f'/tmp/{filename}')
    print ('successfully saved to a local temporary folder')



if __name__ == "__main__":
    while(True):
        image,file_name=sqs_receive(sender_queue_url)
        res=convert_byte_to_image(image,file_name)
        s3_upload(file_name,ip_bucket_name,file_name)
        result= image_classification.classify(f'/tmp/{file_name}')
        result=result.replace(",",' ')
        os.remove(f'/tmp/{file_name}')
        message = {
            'classification_result': result,
            'filename': file_name
        }
        sqs_send(message,receiver_queue_url,file_name)
        s3_message = result
        s3_put(s3_message,file_name,op_bucket_name)
        #print("successfully sent the image classification result of "+key+" to receiver sqs queue")
        print("successfully sent the image classification result of "+file_name+" to receiver sqs queue")




#https://www.geeksforgeeks.org/how-to-upload-and-download-files-from-aws-s3-using-python/
