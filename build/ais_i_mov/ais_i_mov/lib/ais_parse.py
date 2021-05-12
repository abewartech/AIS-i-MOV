#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 14:24:20 2017

@author: rory
"""
import os
import logging
import datetime

log = logging.getLogger('ais_parse')

class AIS_Parser():
    def __init__(self):
        self.routing_key = os.getenv('PRODUCE_KEY')
        self.multi_msg_dict = {}

    def parse_and_seperate(self, msg_chunk, data_logger):
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
        log.debug(msg_chunk)
        msg_chunk = msg_chunk.decode('utf-8')
        msg_chunk = msg_chunk[msg_chunk.index('!'):]
        #Split the chunk into a list of messages
        chunk_list = msg_chunk.split('\n') 
        msg_dict_list = [] 
        
        for msg in chunk_list:            
            if len(msg) < 2:
                continue 
            data_logger.debug(msg)
            msg_dict = self.style_parse(msg)
            msg_dict['server_time'] = datetime.datetime.utcnow().isoformat()
            msg_dict['event_time'] = ''
            msg_dict['routing_key'] = self.routing_key
            msg_dict = self.aivdm_parse(msg, msg_dict) 
            msg_dict_list.append(msg_dict)   
        return msg_dict_list
    
    def aivdm_parse(self, msg, msg_dict):
        if msg.split(',')[1] == '1':                
                msg_dict['multiline'] = False
                msg_dict['message'] = msg 

        #Check if first part of multiline message
        elif msg.split(',')[2] == '1':
            self.multi_msg_dict['msg'] = msg
            self.multi_msg_dict['msg_id'] = msg.split(',')[3]
        
        #Check if second part of multi msg
        elif msg.split(',')[2] == '2':
            msg_dict['multiline'] = True
            #Check if second part belongs with first part
            if msg.split(',')[3] == self.multi_msg_dict['msg_id']:
                msg_dict['message'] = (self.multi_msg_dict['msg'],msg)
                self.multi_msg_dict = {} 
            else:
                log.warning('Dangling multi line message: ' + str(msg))
        else:
            log.warning('Unprocessed msg: ' + str(msg))
        
        return msg_dict
    
    def style_parse():