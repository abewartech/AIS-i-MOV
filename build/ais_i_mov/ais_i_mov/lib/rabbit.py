#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 12:15:15 2017

@author: rory
""" 
import logging
import logging.handlers
import time
import os
import subprocess
import json
import datetime
import pytz
 
from kombu import Connection, Exchange, Producer, Queue, Consumer, binding
from kombu.mixins import ConsumerMixin,ConsumerProducerMixin

log = logging.getLogger('main.lib.rabbit')  
class Rabbit_Consumer(ConsumerMixin):
    '''
    Take each message and do something with it.
    Send the decoded messages to a rabbit broker
    '''
    def __init__(self, message_handler):
        '''
        Initialise the connection to the RMQ server and use the message_handler to 
        process incoming messages. 

        In most cases the message_handler will be the decoder but it could also
        be a filter, neural net, anomoly detector or what not.

        The message handler must accept kombu message object
        '''
        log.info('Setting up RabbitMQ source interface...') 

        # Key to consume from:
        self.rabbit_url = "amqp://{0}:{1}@{2}:{3}/".format(os.getenv('SRC_RABBITMQ_DEFAULT_USER'),
                                                            os.getenv('SRC_RABBITMQ_DEFAULT_PASS'),
                                                            os.getenv('SRC_RABBIT_HOST'),
                                                            os.getenv('SRC_RABBIT_MSG_PORT'))
        log.debug('Source Rabbit is at {0}'.format(self.rabbit_url))
        self.exchange = Exchange(os.getenv('SRC_RABBIT_EXCHANGE'), type="topic")
        self.conn = Connection(self.rabbit_url) #This connection is only used for the dummy queue...
        self.bind_to_keys()
        self.create_test_queue()
        
        self.message_handler = message_handler
        log.info('Consumer init complete')  
        
    def on_message(self, body, message):
        log.info('Msg type %s received: %s',type(body),body)
        if message.delivery_info['redelivered']:
            message.reject()
            return
        else:
            message.ack()
        try:            
            self.message_handler(message)
        except Exception as err:
                log.error('Error in message consumer: {0}'.format(err))
     
    def bind_to_keys(self):
        # takes the list of routing keys in the config file
        # and create a queue bound to them.
        log.info('Creating queue and binding keys')
        topic_binds = []
        keys = json.loads(os.getenv('SRC_KEYS'))
        for key in keys:
            log.info('    -Key: %s',key)
            # NOTE: don't declare queue name. It'll get auto generated and expire after 600 seconds of inactivity
            topic_bind = binding(self.exchange, routing_key=key)
            topic_binds.append(topic_bind)
        queue_name = os.getenv('INSERT_QUEUE')
        self.queue = Queue(name=queue_name,
                        exchange=self.exchange,
                        bindings=topic_binds,
                        max_length = 10000000)

        log.info('Source queues: %s',self.queue)
        return

    def errback(self, exc, interval):
        log.warning('Consumer error: %r', exc)
        log.warning('Retry in %s +1  seconds.', interval)
        time.sleep(float(interval)+1)
        return
 
    def create_test_queue(self):
        # Create a dummy queue on the rabbitmq server. Useful for debugging
        log.info('Creating Source Test Queue')  
        test_q_name = "AAA-{0}-test-consume".format(os.getenv('PROJECT_NAME'))
        queue = Queue(name=test_q_name, 
                        exchange=self.exchange,
                        max_length = 1000, 
                        routing_key=os.getenv('PRODUCE_KEY'))
        queue.maybe_bind(self.conn)
        queue.declare()
        return

    def get_consumers(self, Consumer, channel):
        return [
            Consumer(self.queue, callbacks=[self.on_message], accept=['json']),
        ]
 
class Rabbit_Producer(object):
    '''
    Take each message and do something with it.
    Send the decoded messages to a rabbit broker
    '''
    def __init__(self):
        log.info('Setting up RabbitMQ sink interface...') 
        # Key to consume from:
        self.rabbit_url = "amqp://{0}:{1}@{2}:{3}/".format(os.getenv('SNK_RABBITMQ_DEFAULT_USER'),
                                                            os.getenv('SNK_RABBITMQ_DEFAULT_PASS'),
                                                            os.getenv('SNK_RABBIT_HOST'),
                                                            os.getenv('SNK_RABBIT_MSG_PORT'))
        log.info('Source/Sink Rabbit is at {0}'.format(self.rabbit_url))
        self.exchange = Exchange(os.getenv('SNK_RABBIT_EXCHANGE'), type="topic")
        self.connection = Connection(self.rabbit_url) #This connection is only used for the dummy queue...
 
        self.sink = Producer(exchange=self.exchange,
                              channel=self.connection,
                              serializer ='json' )

        log.info('Producer init complete')  
 

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.queue,
                        on_message=self.on_message,
                        accept={'application/json'},
                        prefetch_count=1)]

    def produce(self, message):
        # This function is meant to be overloaded to provide some kind of functionality
        try:
            log.debug('Sending message to RMQ: ' + str(message))
            producer = self.connection.ensure(self.sink, self.sink.publish, errback=self.errback, interval_start = 1.0)
            routing_key = message['routing_key']
            producer(message, routing_key=routing_key)
        except:
            log.warning('Failed to send msg to rabbitmq')
  
    def errback(self, exc, interval):
        log.warning('Consumer error: %r', exc)
        log.warning('Retry in %s +1  seconds.', interval)
        time.sleep(float(interval)+1)
        return


class Rabbit_ConsumerProducer(ConsumerProducerMixin):

    def __init__(self):
        log.info('Setting up RabbitMQ source/sink interface...') 
        # Key to consume from:
        self.rabbit_url = "amqp://{0}:{1}@{2}:{3}/".format(os.getenv('SRC_RABBITMQ_DEFAULT_USER'),
                                                            os.getenv('SRC_RABBITMQ_DEFAULT_PASS'),
                                                            os.getenv('SRC_RABBIT_HOST'),
                                                            os.getenv('SRC_RABBIT_MSG_PORT'))
        log.debug('Source/Sink Rabbit is at {0}'.format(self.rabbit_url))
        self.exchange = Exchange(os.getenv('SRC_RABBIT_EXCHANGE'), type="topic")
        self.connection = Connection(self.rabbit_url) #This connection is only used for the dummy queue...
        self.bind_to_keys()
        self.create_test_queue()
        self.sink = Producer(exchange=self.exchange,
                              channel=self.connection,
                              serializer ='json' )

        log.info('ConsumerProducer init complete')  
 

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.queue,
                        on_message=self.on_message,
                        accept={'application/json'},
                        prefetch_count=1)]

    def message_processor(self, message):
        # This function is meant to be overloaded to provide some kind of functionality
        msg_dict = json.loads(message.body.decode('utf-8'))
        log.info('Message :' + str(msg_dict))
        return msg_dict
 

    def on_message(self, message):
        log.info('Msg type %s received: %s',type(message.body),message.body)
        if message.delivery_info['redelivered']:
            message.reject()
            return
        else:
            message.ack()
        try:            
            result = self.message_processor(message)
            producer = self.connection.ensure(self.sink, self.sink.publish, errback=self.errback, interval_start = 1.0)
            producer(result, routing_key=os.getenv('PRODUCE_KEY'))

        except Exception as err:
                log.error('Error in message processor: {0}'.format(err))


    def bind_to_keys(self):
        # takes the list of routing keys in the config file
        # and create a queue bound to them.
        log.info('Creating queue and binding keys')
        topic_binds = []
        keys = json.loads(os.getenv('SRC_KEYS'))
        for key in keys:
            log.info('    -Key: %s',key)
            topic_bind = binding(self.exchange, routing_key=key)
            topic_binds.append(topic_bind)
        self.source_keys = topic_binds      
        queue_name = os.getenv('SRC_QUEUE')
        self.queue = Queue(name=queue_name,
                        exchange=self.exchange,
                        bindings=self.source_keys,
                        max_length = 10000000)
        self.queue.maybe_bind(self.connection)
        self.queue.declare()
        log.info('Source queues: %s',self.queue)
        return

    def errback(self, exc, interval):
        log.warning('Consumer error: %r', exc)
        log.warning('Retry in %s +1  seconds.', interval)
        time.sleep(float(interval)+1)
        return
 
    def create_test_queue(self):
        # Create a dummy queue on the rabbitmq server. Useful for debugging
        log.info('Creating Source Test Queue')  
        test_q_name = "AAA-{0}-test-source".format(os.getenv('PROJECT_NAME'))
        queue = Queue(name=test_q_name, 
                        exchange=self.exchange,
                        max_length = 100, 
                        routing_key=self.source_keys)
        queue.maybe_bind(self.connection)
        queue.declare()

        log.info('Creating Sink Test Queue')  
        test_q_name = "AAA-{0}-test-sink".format(os.getenv('PROJECT_NAME'))
        queue = Queue(name=test_q_name, 
                        exchange=self.exchange,
                        max_length = 100, 
                        routing_key=os.getenv('PRODUCE_KEY'))
        queue.maybe_bind(self.connection)
        queue.declare()

        return