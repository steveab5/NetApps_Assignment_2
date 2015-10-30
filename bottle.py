#!/usr/bin/env python
import pika
import json
import shelve
import fnmatch
import unicodedata
import RPi.GPIO as GPIO

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))

channel = connection.channel()

channel.queue_declare(queue='rpc_queue')

s = shelve.open("bottle_shelf.shelve", writeback = True)

def display_Binary_LEDS(n):

   GPIO.setmode(GPIO.BOARD)
   GPIO.setwarnings(False)
   GPIO.setup(11,GPIO.OUT)
   GPIO.setup(12,GPIO.OUT)
   GPIO.setup(13,GPIO.OUT)
   GPIO.setup(15,GPIO.OUT)

   if n == 0:
        GPIO.output(11, 0)
        GPIO.output(12, 0)
        GPIO.output(13, 0)
        GPIO.output(15, 0)
 
   if n == 1:
        GPIO.output(11, 1)
        GPIO.output(12, 0)
        GPIO.output(13, 0)
        GPIO.output(15, 0)

   if n == 2:
        GPIO.output(11, 0)
        GPIO.output(12, 1)
        GPIO.output(13, 0)
        GPIO.output(15, 0)

   if n == 3:
        GPIO.output(11, 1)
        GPIO.output(12, 1)
        GPIO.output(13, 0)
        GPIO.output(15, 0)

   if n == 4:
        GPIO.output(11, 0)
        GPIO.output(12, 0)
        GPIO.output(13, 1)
        GPIO.output(15, 0)

   if n == 5:
        GPIO.output(11, 1)
        GPIO.output(12, 0)
        GPIO.output(13, 1)
        GPIO.output(15, 0)

   if n == 6:
        GPIO.output(11, 0)
        GPIO.output(12, 1)
        GPIO.output(13, 1)
        GPIO.output(15, 0)

   if n == 7:
        GPIO.output(11, 1)
        GPIO.output(12, 1)
        GPIO.output(13, 1)
        GPIO.output(15, 0)

   if n == 8:
        GPIO.output(11, 0)
        GPIO.output(12, 0)
        GPIO.output(13, 0)
        GPIO.output(15, 1)

   if n == 9:
        GPIO.output(11, 1)
        GPIO.output(12, 0)
        GPIO.output(13, 0)
        GPIO.output(15, 1)

   if n == 10:
        GPIO.output(11, 0)
        GPIO.output(12, 1)
        GPIO.output(13, 0)
        GPIO.output(15, 1)

   if n == 11:
        GPIO.output(11, 1)
        GPIO.output(12, 1)
        GPIO.output(13, 0)
        GPIO.output(15, 1)
      
   if n == 12:
        GPIO.output(11, 0)
        GPIO.output(12, 0)
        GPIO.output(13, 1)
        GPIO.output(15, 1)

   if n == 13:
        GPIO.output(11, 1)
        GPIO.output(12, 0)
        GPIO.output(13, 1)
        GPIO.output(15, 1)

   if n == 14:
        GPIO.output(11, 0)
        GPIO.output(12, 1)
        GPIO.output(13, 1)
        GPIO.output(15, 1)

   if n == 15:
        GPIO.output(11, 1)
        GPIO.output(12, 1)
        GPIO.output(13, 1)
        GPIO.output(15, 1)

def on_request(ch, method, props, body):
    print
    print "[.] Received:"
    print " ", body
    Received_JSON_Object = json.loads(body)
    response = json.dumps({"Status" : "Not Found"})    
   
    if (Received_JSON_Object['Action'] == "push"):

	response = json.dumps({"Status" : "Success"})
	msgID = Received_JSON_Object['MsgID']
	s[str(msgID)] = Received_JSON_Object 
	s.sync() 
    
    else:
	keylist = s.keys()
	for k in keylist:
   	    if (s[k]['Author']) == Received_JSON_Object['Author'] or Received_JSON_Object['Author'] == '':
		if str(s[k]['Age']) == str(Received_JSON_Object['Age']) or Received_JSON_Object['Age'] == '': 
		    if (s[k]['Subject']) == Received_JSON_Object['Subject'] or Received_JSON_Object['Subject'] == '':
			if (s[k]['Message']) == Received_JSON_Object['Message'] or Received_JSON_Object['Message'] == '':
			    response = json.dumps(s[k])
			    if Received_JSON_Object['Action'] == "pull":
				del s[k]
			    break

    
    keylist = s.keys()
    decimal_num = len(keylist)
    print
    print 'Number of messages: ', decimal_num 
    print
    print "[x] Replying with:"
    print " ", response  

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                     props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_request, queue='rpc_queue')

print "[x] Awaiting RPC requests "
channel.start_consuming()


