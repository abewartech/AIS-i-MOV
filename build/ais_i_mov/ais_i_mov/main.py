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
import lib.ais_parse

# This whole thing is supposed to take the raw AIS, do a high level split on it: 
    # - Meta-data 
    # - Raw AIS
    # - Header/Footer
    # - Routing key
# This is then written to a log file and passed to rabbit.


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
    try:
        log.info('Streaming AIS...')
        while True:
            data_chunk = sock.recv(int(os.getenv('CHUNK_BYTES')))
            log.warning('----------------')
            chunk_len = len(data_chunk)
            if chunk_len <  150:
                row_len = chunk_len
            else:
                row_len = 150
            log.warning('Chunk size: {0}'.format(chunk_len))
            log.warning('Chunk start: {0} ...'.format(data_chunk[0:row_len]))
            log.warning('Chunk end: ... {0}'.format(data_chunk[row_len:]))
            # if len(data_chunk) > 2:
                # msg_list = ais_parser.parse_and_seperate(data_chunk,data_logger)
                # for msg in msg_list:
                    # rabbit_publisher.produce(msg)
            # else:
                # continue
    except:
        log.error('Error in AIS streaming:' + traceback.format_exc())
        log.error('Data: ' + str(data_chunk))
    finally:
        log.warning('Closing socket')
        sock.close() 
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
        # os._exit(0)
    except Exception as error:
        log.error('Other exception. Exiting with code 1...')
        log.error(traceback.format_exc())
        log.error(error)
        # os._exit(1)