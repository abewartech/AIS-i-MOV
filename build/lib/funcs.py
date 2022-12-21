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
 
from threading import Timer
from kombu import Connection, Exchange, Producer, Queue, Consumer, binding


log = logging.getLogger('main.funcs') 

def read_env_vars():
    '''
    Read environ variables and return as dict. 

    This is to replace the config file function in preperation for rancher, where all config is handled by env vars.
    '''
    log.debug('Reading environment variables...')
    CFG = {}

    #Project
    CFG['project_name'] = os.getenv('PROJECT_NAME')
    # Source
    CFG['src_rabbit_port'] = os.getenv('SRC_RABBIT_MSG_PORT')
    CFG['src_rabbit_user'] = os.getenv('SRC_RABBITMQ_DEFAULT_USER')
    CFG['src_rabbit_pw'] = os.getenv('SRC_RABBITMQ_DEFAULT_PASS')
    CFG['src_rabbit_host'] = os.getenv('SRC_RABBIT_HOST')
    CFG['src_routing_exch'] = os.getenv('SRC_RABBIT_EXCHANGE')
    CFG['src_keys'] = os.getenv('SRC_KEYS')
    CFG['src_'] = os.getenv('SRC_QUEUE')
    CFG['src_'] = os.getenv('QUEUE_MAX_LENGTH')
    CFG['src_'] = os.getenv('ON_ERROR_DROP_MSGS')

    #Sink
    CFG['snk_rabbit_host'] = os.getenv('SRC_RABBIT_HOST')
    CFG['snk_rabbit_port'] = os.getenv('SRC_RABBIT_MSG_PORT')
    CFG['snk_rabbit_user'] = os.getenv('SRC_RABBITMQ_DEFAULT_USER')
    CFG['snk_rabbit_pass'] = os.getenv('SRC_RABBITMQ_DEFAULT_PASS')
    CFG['snk_rabbit_exch'] = os.getenv('SRC_RABBIT_EXCHANGE')  

    log.info('Config: {0}'.format(CFG))
    return CFG
 

class Watchdog(Exception):
    '''
    Watchdog timer implementation used to restart the container 
    when it fails to publish a new message within X seconds.

    Taken from
    https://stackoverflow.com/questions/16148735/how-to-implement-a-watchdog-timer-in-python?noredirect=1&lq=1
    ''' 
    def __init__(self, timeout, userHandler=None):  # timeout in seconds
        log.info(f'Setting up watchdog timer for {timeout} seconds.') 
        self.timeout = timeout
        self.handler = userHandler if userHandler is not None else self.defaultHandler
        self.timer = Timer(self.timeout, self.handler)
        self.timer.start()

    def reset(self):
        self.timer.cancel()
        self.timer = Timer(self.timeout, self.handler)
        self.timer.start()

    def stop(self):
        self.timer.cancel()

    def defaultHandler(self):
        raise self