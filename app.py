import os
from flask import Flask, flash, request, redirect, url_for,render_template
from werkzeug.utils import secure_filename
import logging
import time
import boto3
import json
import re
import base64
from botocore.exceptions import ClientError

#global
res=""
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg'}
# app = Flask(__name__,
#             template_folder=os.path.join('/Users/priyadarshiniramakrishnan/Documents/CloudComputing/cse546-IAAS'))
app = Flask(__name__,template_folder=os.path.join('/home/ubuntu/project1'))
#app = Flask(__name__,template_folder=os.path.join('/Users/priyadarshiniramakrishnan/Documents/CloudComputing/cse546-IAAS'))
app.config['secret_key'] = 'myfile'
#sender_queue_url = 'https://sqs.us-east-1.amazonaws.com/836862805331/image_sender.fifo'
sender_queue_url = 'https://sqs.us-east-1.amazonaws.com/836862805331/sender'
#receiver_queue_url = 'https://sqs.us-east-1.amazonaws.com/836862805331/receiver.fifo'
receiver_queue_url='https://sqs.us-east-1.amazonaws.com/836862805331/receiver'
sqs = boto3.client('sqs',aws_access_key_id='#########',
                   aws_secret_access_key = '############',
                   region_name="us-east-1")

def sqs_send(message,ID):

    response = sqs.send_message(
        QueueUrl=sender_queue_url,
        #MessageGroupId=ID,
        DelaySeconds=0,
        MessageAttributes={
            'Title': {
                'DataType': 'String',
                'StringValue': 'Input Image location'
            },
            'Author': {
                'DataType': 'String',
                'StringValue': 'web_tier.py'
            }
        },
        MessageBody=(json.dumps(message)),
    )
    print('sent sqs message')

def sqs_receive(url,filename):

    no_msg = True
    no_file = True
    while(no_msg and no_file):
        response = sqs.receive_message(
            QueueUrl=url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=['All'],
            VisibilityTimeout=10,
            WaitTimeSeconds=10)

        if len(response.get("Messages", []))>0:
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            if message!=None:
                body = message["Body"]
                body = json.loads(body)
                result = body["classification_result"]
                file_name = body["filename"]
                if file_name==filename:
                    no_file=False
                    no_msg=False
                    sqs.delete_message(
                        QueueUrl=url,
                        ReceiptHandle=receipt_handle
                    )
                    print('Received and deleted message for object: %s' % filename)

                else:
                    message= None
                    receipt_handle=None
                    print("No messages available..polling")

    return result,file_name


def allowed_file(filename):
    return '.' in filename and \
        filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    return render_template('WebMain.html')

#/https://flask.palletsprojects.com/en/2.2.x/patterns/fileuploads/

@app.route('/', methods=['POST','GET'])
def upload_file():
    global res
    res = []
    if request.method == 'POST':
        if 'myfile' not in request.files:
            print('No image found')
            return 'No image uploaded'

    for file in request.files.getlist('myfile'):
        # If the user does not select a file, the browser submits an empty file without a filename.
        if file.filename == '':
            print('No selected file')
            return 'No image uploaded'
        if file and allowed_file(file.filename) and file.filename!='':
            filename = secure_filename(file.filename)
            file_bytes_string = base64.encodebytes(file.read()).decode('utf-8')
            message = {
                'image': file_bytes_string,
                'filename': file.filename
            }
            sqs_send(message,str(filename))
    for file in request.files.getlist('myfile'):
        if file and allowed_file(file.filename) and file.filename!='':
            filename = secure_filename(file.filename)
            result,file_name = sqs_receive(receiver_queue_url,str(filename))
            print(result)
            res.append(result)
    return res
    #time.sleep(3)
# else: return 'No image'



# @app.route('/', methods=['GET'])
# def test():
#     return render_template('index.html', res=res)

#return redirect(url_for('upload_file', name=filename))

if __name__=='__main__':
    app.run(debug = True, threaded=True)

#References:
#https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
#https://stackoverflow.com/questions/39491420/python-jsonexpecting-property-name-enclosed-in-double-quotes
