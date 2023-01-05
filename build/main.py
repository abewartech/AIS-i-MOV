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
import re

from kombu import Connection, Exchange, Queue, binding

import lib.funcs
import lib.rabbit
import lib.ais_parse

# This whole thing is supposed to take the raw AIS, do a high level split on it: 
    # - Meta-data 
    # - Raw AIS
    # - Header/Footer
    # - Routing key
# This is then written to a log file and passed to rabbit.

def WatchDogHandler():
    log.error('No messages published within 300 seconds. Exiting container...')
    os._exit(1)

log = logging.getLogger('main')
# log.setLevel('DEBUG')
def setup_logging():
    '''
    Setup some nice logging to store incoming data
    '''
    logger = logging.getLogger(os.getenv('LOG_NAME'))
    if not len(logger.handlers):
        logger.setLevel(logging.INFO)
        log_path = os.getenv('LOG_DIR')
        log_file = os.getenv('LOG_NAME')
        if not os.path.exists(log_path): 
            os.mkdir(log_path)
        log_file_handler = logging.handlers.TimedRotatingFileHandler(os.path.join(log_path,log_file), when='midnight')
        log_file_handler.setFormatter( logging.Formatter('%(asctime)s: %(message)s') )
        log_file_handler.setLevel(logging.DEBUG)
        logger.addHandler(log_file_handler)
    return logger

def read_socket(data_logger):
    # Create a TCP/IP socket
    log.info('Opening socket') 
    sock = socket.socket()
    log.info('Setting socket timeout to %s seconds.',os.getenv('SOCKET_TIMEOUT'))
    sock.settimeout(int(os.getenv('SOCKET_TIMEOUT')))
    server_address = (os.getenv('SOURCE_HOST'),int(os.getenv('SOURCE_PORT')))
    log.info('Connecting to '+str(server_address)) 
    sock.connect(server_address)
    
    # Create RabbitMQ publisher
    rabbit_publisher = lib.rabbit.Rabbit_Producer()
    ais_parser = lib.ais_parse.AIS_Parser()
    watchdog = lib.funcs.Watchdog(300, WatchDogHandler)   
    try:
        log.info('Streaming AIS...') 
        while True:
            # https://stackoverflow.com/questions/47758023/python3-socket-random-partial-result-on-socket-receive
            data_chunk = b''
            while True:
                try:
                    chunk = sock.recv(int(os.getenv('CHUNK_BYTES')))
                    
                    log.debug('Chunk received')
                    valid_chunk = re.search(b'[0]\*[0-9a-fA-F]+$', chunk)
                    # if not chunk:
                    #     log.debug('Complete chunk, reading more: len = {}'.format(len(chunk)))
                    #     break
                    if valid_chunk is not None:
                        log.debug('Complete chunk, reading more: len = {}'.format(len(chunk)))
                        break
                    data_chunk += chunk
                    
                except socket.error:
                    # sock.close() 
                    break 
            log.debug('----------------')
            chunk_len = len(data_chunk)
            if chunk_len <  316:
                row_len = chunk_len
            else:
                row_len = 316
            log.debug('Chunk size: {0}'.format(chunk_len))
            log.debug('Chunk start: {0} ...'.format(data_chunk[0:row_len+1]))
            log.debug('Chunk end: ... {0}'.format(data_chunk[-row_len-1:]))
            if len(data_chunk) > 2:
                try:
                    msg_list = ais_parser.parse_and_seperate(data_chunk,data_logger)
                    watchdog.reset()
                except:
                    log.info('Problem parsing message')
                for msg in msg_list:
                    rabbit_publisher.produce(msg)
                    # log.info(msg['ais'])
                    pass
                
            else:
                continue
    except:
        log.error('Error in AIS streaming:' + traceback.format_exc())
        log.error('Last MSG: ' + str(msg))
    finally:
        log.warning('Closing socket')
        # sock.close() 
        time.sleep(10)
 
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
        time.sleep(10)


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
        os._exit(0)
    except Exception as error:
        log.error('Other exception. Exiting with code 1...')
        log.error(traceback.format_exc())
        log.error(error)
        os._exit(1)