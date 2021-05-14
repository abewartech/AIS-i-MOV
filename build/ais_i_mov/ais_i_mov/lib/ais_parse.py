#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  5 14:24:20 2017

@author: rory
"""
import os
import logging
import datetime
import traceback
from re import X

log = logging.getLogger('ais_parse')

class AIS_Parser():
    # This takes a chunk of raw AIS+metadata messages and then fires off a dictionary of encoded 
    # AIS message + parsed metadata to rabbit. 
    # Parsing style depends on the source of data and is controlled by the .env vars
    def __init__(self):
        self.routing_key = os.getenv('PRODUCE_KEY')
        self.ais_meta_style = os.getenv('AIS_STYLE')
        log.debug('Sending to R-Key: {0}'.format(self.routing_key))
        log.debug('AIS Style: {0}'.format(self.ais_meta_style))
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

        # This probably needs a little more thinking. How to index chunks when there are different metadata styles? 
        # How can I guarentee that the line is started in the right place?
        if self.ais_meta_style == 'IMIS':
            msg_chunk = msg_chunk[msg_chunk.index('\r')+1:]
            chunk_list = msg_chunk.split('\n') 
        elif self.ais_meta_style == 'NONE':
            msg_chunk = msg_chunk[msg_chunk.index('!'):]
            chunk_list = msg_chunk.split('\n') 
        else: 
            pass
        #Split the chunk into a list of messages
        # chunk_list = msg_chunk.split('\n') 
        msg_dict_list = [] 
        
        for msg in chunk_list:    
            try:        
                if len(msg) < 2:
                    continue 
                data_logger.debug(msg)
                msg_dict = self.style_parse(msg)
                log.debug(msg_dict)
                msg_dict = self.aivdm_parse(msg_dict) 
                msg_dict_list.append(msg_dict)   
            except:
                log.warning('Problem while parsing AIS message: {0}'.format(str(msg)))
                log.warning('Parsing Error:' + traceback.format_exc()) 
                log.warning('Dict: {0}'.format(msg_dict))
                log.warning('Multi-Dict: {0}'.format(self.multi_msg_dict))
        return msg_dict_list
    
    def aivdm_parse(self, msg_dict):
        msg = msg_dict['ais']
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
    
    def style_parse(self, msg):
        # IMIS metadata format. 
        # \s:CSIR_000,q:u,c:1620731505,i:|X=0|D=1|T=44327.4781489815|P=10.0.100.6:12113|R=IN|E=10000000000000000000|*48\!AIVDM,1,1,,B,33ku82U000OGsfHH:Uv`9j3J0>@<,0*68                         
        # \g:1-2-1159,s:CSIR,c:1620731662,i:|X=1|D=1|T=44327.4683069097|P=10.0.100.6:12113|R=IN|*46\!AIVDM,2,1,9,A,53Fted42?II@D5=:2204h8Ub2222222222222216:`?1>5D80B0hDh@S0CPh,0*3F            
        # \g:2-2-1159*51\!AIVDM,2,2,9,A,H8888888880,2*5D  
        # s: Source
        # q: 
        # c: unix timestamp
        # i: IMIS metadata
            # X: Data Source?
            # D: Delay flag?
            # T: IMIS timestamp?
            # P: IP Address of source
            # R: Direction of message? 

        
        # Bog Standard metadata format
        # !ABVDM,1,1,,B,13=fod0vQv1B5LgdH:vMAJdB00Sa,0*42
        # !BSVDM,2,1,8,B,5E@;DN02AO;3<HMOJ20Lht84j1A9E=B222222216O@a@M00HtGQSl`3lQ1DT,0*75
        # !BSVDM,2,2,8,B,p8888888880,2*7E
        # !ABVDM,1,1,,B,HF<nO80d4v0HtpN0pvs40000000,2*62
        # !ABVDM,1,1,,B,B8u:Qa0000DwdMs8?LrDio053P06,0*59
        
        log.debug('Parsing: {0}'.format(msg))
        parsed_line = {}
        if self.ais_meta_style == 'IMIS':
            meta = msg[: msg.index('\!')+1]
            ais = msg[msg.index('\!')+1 :]
            meta_list = meta.strip('\\').split(',')
            meta_dict = {}
            for item in meta_list:
                meta_dict[item[: item.index(':')]] = item[item.index(':') +1:]

            parsed_line['server_time'] = datetime.datetime.utcnow().isoformat()
            parsed_line['header'] = meta_dict
            parsed_line['routing_key'] = self.routing_key
            try:
                parsed_line['event_time'] =  datetime.datetime.fromtimestamp(int(meta_dict['c']))
            except:
                log.debug('No timestamp on this message')
            parsed_line['ais'] = ais

        # elif self.ais_style == 'NONE':
        #     parsed_line['server_time'] = datetime.datetime.utcnow().isoformat()
        #     parsed_line['event_time'] = ''
        #     parsed_line['routing_key'] = self.routing_key
        else: 
            parsed_line['server_time'] = datetime.datetime.utcnow().isoformat()
            parsed_line['event_time'] = ''
            parsed_line['routing_key'] = self.routing_key 
        return parsed_line   

