    #!/usr/bin/env python2.7
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 24 15:05:19 2017

@author: rory
"""
import sys
import time
import argparse
import logging 
import os
import traceback
import socket
import datetime

from kombu import Connection, Exchange, Queue, binding

import lib.funcs
import lib.rabbit

# This whole thing is supposed to take the raw AIS, do a high level split on it: 
    # - Meta-data 
    # - Raw AIS
    # - Header/Footer
    # - Routing key
# This is then written to a log file and 


log = logging.getLogger('main')
# log.setLevel('DEBUG')
def setup_logging():
    '''
    Setup some nice logging to store incoming data
    '''
    logger = logging.getLogger(os.getenv('LOG_NAME'))
    if not len(logger.handlers):
        logger.setLevel(logging.DEBUG)
        log_path = os.getenv('LOG_DIR')
        log_file = os.getenv('LOG_NAME')
        if not os.path.exists(log_path): 
            os.mkdir(log_path)
        log_file_handler = logging.handlers.TimedRotatingFileHandler(log_path+log_file, when='midnight')
        log_file_handler.setFormatter( logging.Formatter('%(asctime)s: %(message)s') )
        log_file_handler.setLevel(logging.DEBUG)
        logger.addHandler(log_file_handler)
    return logger

class AIS_Parser():
    def __init__(self):
        self.routing_key = os.getenv('PRODUCE_KEY')
        self.multi_msg_dict = {}

    def parse_and_seperate(self, msg_chunk,data_logger):
        # Take a chunk of messages and split them up line by line
        # Log incoming messages
        # Parse out the header and footer info
        # group multiline messages
        # return a list of dicts 

        # Example Data
        # !ABVDM,1,1,,B,13=fod0vQv1B5LgdH:vMAJdB00Sa,0*42
        # !BSVDM,2,1,8,B,5E@;DN02AO;3<HMOJ20Lht84j1A9E=B222222216O@a@M00HtGQSl`3lQ1DT,0*75
        # !BSVDM,2,2,8,B,p8888888880,2*7E
        # !ABVDM,1,1,,B,HF<nO80d4v0HtpN0pvs40000000,2*62
        # !ABVDM,1,1,,B,B8u:Qa0000DwdMs8?LrDio053P06,0*59

        #Place start of message at start of chunk
        msg_chunk = msg_chunk[msg_chunk.index('!'):]
        #Split the chunk into a list of messages
        chunk_list = msg_chunk.split('\n') 
        msg_dict_list = [] 
        
        for msg in chunk_list:
            data_logger.debug(msg)
            msg_dict = {}
            msg_dict['event_time'] = datetime.datetime.utcnow().isoformat()
            msg_dict['routing_key'] = self.routing_key

            if msg.split(',')[1] == '1':                
                msg_dict['multiline'] = False
                msg_dict['message'] = msg
                msg_dict_list.append(msg_dict)

            #Check if first part of multiline message
            elif msg.split(',')[2] == '1':
                self.multi_msg_dict['msg'] = msg
                self.multi_msg_dict['msg_id'] = msg.split(',')[3]
            
            #Check if second part of multi msg
            elif msg.split(',')[2] == '2':
                msg_dict['multiline'] = True
                #Check if second part belongs with first part
                if msg.split(',')[3] == self.multi_msg_dict['msg_id']:
                    msg_dict['message'] = (msg, self.multi_msg_dict['msg'])
                    self.multi_msg_dict = {}
                    msg_dict_list.append(msg_dict)
                else:
                    log.warning('Dangling multi line message: ' + str(msg))
            else:
                log.warning('Unprocessed msg: ' + str(msg))
        return msg_dict_list

def read_socket(data_logger):
    # Create a TCP/IP socket
    log.info('Opening socket')
    sock = socket.socket()
    log.info('Setting socket timeout to %s seconds.',os.getenv('SOCKET_TIMEOUT'))
    sock.settimeout(int(os.getenv('SOCKET_TIMEOUT')))
    server_address = (os.getenv('SOURCE_HOST'),os.getenv('SOCKET_TIMEOUT'))
    log.info('Connecting to '+str(server_address))
    sock.connect(server_address)

    # Create RabbitMQ publisher
    rabbit_publisher = lib.rabbit.Rabbit_Producer()
    try:
        log.info('Streaming AIS...')
        while True:
            data_chunk = sock.recv(int(os.getenv('CHUNK_BYTES')))
            msg_list = parse_and_seperate(data_chunk,data_logger)
            for msg in msg_list:
                rabbit_publisher.produce(msg)
    except:
        log.error('Error in AIS streaming:')
        log.error(traceback.format_exc())
    finally:
        log.warning('Closing socket')
        sock.close() 
 
def do_work():  
    '''
    This opens a socket to the server that is feeding AIS.
    Collect multiline messages into a single dict.
    Adds a routing key, server time etc to the message
    Publishes message onto RabbitMQ
    '''
    log.info('Getting ready to do work...') 
    data_logger = setup_logging()
    try:
        while True:
            read_socket(data_logger)
    except:
        log.error('Error in main loop:') 
        log.error(traceback.format_exc())


    log.info('Worker shutdown...')

def main(args):
    '''
    Setup logging, and args, then "do_work"
    '''
    logging.basicConfig(
        stream=sys.stdout,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        level=getattr(logging, args.loglevel))

    log.setLevel(getattr(logging, args.loglevel))
    log.info('ARGS: {0}'.format(ARGS)) 
    do_work()
    log.warning('Script Ended...') 

if __name__ == "__main__":
    '''
    This takes the command line args and passes them to the 'main' function
    '''
    PARSER = argparse.ArgumentParser(
        description='Run the DB inserter')
    PARSER.add_argument(
        '-f', '--folder', help='This is the folder to read.',
        default = None, required=False)
    PARSER.add_argument(
        '-ll', '--loglevel', default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help="Set log level for service (%s)" % 'INFO')
    ARGS = PARSER.parse_args()
    try:
        main(ARGS)
    except KeyboardInterrupt:
        log.warning('Keyboard Interrupt. Exiting...')
        # os._exit(0)
    except Exception as error:
        log.error('Other exception. Exiting with code 1...')
        log.error(traceback.format_exc())
        log.error(error)
        # os._exit(1)