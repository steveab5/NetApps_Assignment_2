#!/usr/bin/env python
import pika
import uuid
import sys
import getopt
import json
import random
import shelve
import unicodedata

class Pebble():
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                #host='172.30.147.30'))
		host='localhost'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(self.on_response, no_ack=True,
                                   queue=self.callback_queue)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, n):
	msgID = str(random.randint(0, 1000000))
	action = ''
	subject = ''
	message = ''
	Qage = ''
	Qsubject = ''
	Qmessage = ''
	Qauthor = ''
	jsonString = ''

	try:
	    opts, args = getopt.getopt(sys.argv[1:],"ha:s:m:",["Qa=","Qs=","Qm=","QA="])
	except getopt.GetoptError:
	    print 'pebble.py -a <action> -s <subject> -m <message>'
	    sys.exit(2)
	for opt,arg in opts:
	    if opt=='-h':
		print 'pebble.py -a <action> -s <subject> -m <message>'
		sys.exit()
	    elif opt in ("-a"):
		action = arg
	    elif opt in ("-s"):
		subject = arg
	    elif opt in ("-m"):
		message = arg
	    elif opt in ("--Qa"):
		Qage = arg
	    elif opt in ("--Qs"):
		Qsubject = arg
	    elif opt in ("--Qm"):
		Qmessage = arg
	    elif opt in ("--QA"):
		Qauthor = arg
#	print 'Action is ', action
#	print 'Subject is ', subject
#	print 'Message is ', message
#	print 'Query Action is ', Qaction
#	print 'Query Subject is ', Qsubject
#	print 'Query Message is ', Qmessage
#	print msgID

	if action == 'push':
	    jsonString = {
	        'Action': action,
		'Author': 'Brian Ribeiro',
		'Age': 21,
		'MsgID': msgID,
		'Subject': subject,
		'Message': message,
	    }
	else:
	    jsonString = {
	        'Action': action,
		'Age': Qage,
		'MsgID': msgID, #REMOVE THIS@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
		'Author': Qauthor,
		'Subject': Qsubject,
		'Message': Qmessage,
	    }

#	print 'JSON', jsonString

	jsonSend = json.dumps(jsonString)
	
	print
	print "[x] Sending:"
	print " ", jsonSend

        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',
                                   routing_key='rpc_queue',
                                   properties=pika.BasicProperties(
                                         reply_to = self.callback_queue,
                                         correlation_id = self.corr_id,
                                         ),
                                   body=jsonSend)
        while self.response is None:    
            self.connection.process_data_events()

	if action == 'pull' or action == 'pullr':
	    jsonReply = json.loads(self.response)
	    replyID = jsonReply['MsgID']
	    s = shelve.open("pebble-shelf.db", writeback=True)
	    #try:
	    s[unicodedata.normalize('NFKD',replyID).encode('ascii','ignore')] = self.response
	    #s['server1'] = {'Hostname':'Server1', 'RAM':128}
	    s.sync()
	    print
	    print 'SHELVE:'
	    #print s[unicodedata.normalize('NFKD',replyID).encode('ascii','ignore')]
	    #except:
	    #print
	    #print 'ERROR: Received message with wrong formatting'
	    s.close()

        return self.response


pebble_rpc = Pebble()

response = pebble_rpc.call(30)
print
print "[.] Received:"
print " ",response
print
