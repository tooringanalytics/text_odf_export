#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Created on 26 Jun 2013

@author: Anshuman
'''

import re
import config

class FCEContext(object):

    def __init__(self,
                s_settings_file,
                dd_store,
                s3_store,
                s_app_dir,
                s_exchange, 
                s_odf_dd):
        
        super(FCEContext, self).__init__()

        # Store a copy of the config        
        self.config = config.Config()
        
        self.config.read_settings(s_settings_file)
        
        self.dd_store = dd_store
        
        self.s3_store = s3_store
        
        self.s_app_dir = s_app_dir
        
        self.s_exchange = s_exchange
        
        self.s_exchange_basename = dd_store.get_exchange_basename(s_exchange)
        
        self.s_odf_dd = s_odf_dd 
        
        self.s_odf_basename = dd_store.get_odf_basename(s_odf_dd)
        
        self.s_symbol = dd_store.get_odf_symbol(self.s_odf_basename)
        
        self.odf_obj = self.load_odf(self.config, self.s_exchange_basename, self.s_odf_dd)
        
        self.init_fce_paths(self.s3_store, self.s_app_dir, self.s_exchange_basename, self.s_symbol, self.s_odf_basename)

        self.init_fifo_paths(self.s_exchange_basename, self.s_odf_basename)
    
    def load_public_from_odf(self, odf_obj, config):
    
        # Get the common ODF headers
        odf_gmt_offset = odf_obj.get_header_value(config.gmt_offset_storloc)
        odf_trading_start_recno = odf_obj.get_header_value(config.trading_start_recno_storloc)
        odf_trading_recs_perday = odf_obj.get_header_value(config.trading_recs_perday_storloc)
        odf_idf_currency = odf_obj.get_header_value(config.idf_currency_storloc)
        odf_idf_currency_max_decimals = odf_obj.get_header_value(config.idf_currency_max_decimals_storloc)
        odf_split_factor = odf_obj.get_header_value(config.split_factor_storloc)
        odf_currency_value_of_point = odf_obj.get_header_value(config.currency_value_of_point_storloc)
    
        # Get the 'specific' odf headers, which may be missing
        odf_tick = odf_obj.get_header_value(config.tick_storloc)
        odf_ohlc_divider = odf_obj.get_header_value(config.ohlc_divider_storloc)
        odf_last_fced_recno = odf_obj.get_header_value(config.last_fced_recno_storloc)
        odf_highest_recno = odf_obj.get_header_value(config.highest_recno_storloc)
        odf_highest_recno_close = odf_obj.get_header_value(config.highest_recno_close_storloc)
        odf_prev_highest_recno_close = odf_obj.get_header_value(config.prev_highest_recno_close_storloc)
    
        # Save this ODF's common headers to public values
        config.gmt_offset = odf_gmt_offset
        config.trading_start_recno = odf_trading_start_recno
        config.trading_recs_perday = odf_trading_recs_perday
        config.idf_currency = odf_idf_currency
        config.idf_currency_max_decimals = odf_idf_currency_max_decimals
        config.split_factor = odf_split_factor
        config.currency_value_of_point = odf_currency_value_of_point

        # Save the ODf-specific header to public values
        config.tick = odf_tick
        config.ohlc_divider = odf_ohlc_divider
        config.last_fced_recno = odf_last_fced_recno
        config.highest_recno = odf_highest_recno
        config.highest_recno_close = odf_highest_recno_close
        config.prev_highest_recno_close = odf_prev_highest_recno_close

    def load_odf(self, config, s_exchange_basename, s_odf_dd):
        
        ''' Load the odf object
        '''
        odf_obj = self.dd_store.open_odf(s_exchange_basename, s_odf_dd)
        '''
        Load public variables from odf_obj
        '''
        self.load_public_from_odf(odf_obj, config)
        return odf_obj
        
    def init_fifo_paths(self, s_exchange_basename, s_odf_basename):
        ''' Make FIFO name for this ODF from the ODF's basename
        '''
        self.s_fifo_dd = self.dd_store.get_fifo_path(s_exchange_basename, s_odf_basename)
        self.s_fifo_basename = self.dd_store.get_fifo_basename(self.s_fifo_dd)

    def init_fce_paths(self, s3_store, s_app_dir, s_exchange_basename, s_symbol, s_odf_basename):
        
        matchobj = re.match(r'^(.*\-18)(\d+).*$', s_odf_basename)

        s_jsunnoon = matchobj.group(2)

        s_fce_basename = matchobj.group(1)

        self.odf_jsunnoon = int(s_jsunnoon)

        self.prev_odf_jsunnoon = self.odf_jsunnoon - 7

        self.s_fce_header_file_name = ''.join([s_fce_basename, str(self.odf_jsunnoon), '01', '.fce'])

        self.s_prev_fce_header_file_name = ''.join([s_fce_basename, str(self.prev_odf_jsunnoon), '01', '.fce'])
        
        self.s_fce_remote_prefix = s3_store.rsep.join([s_exchange_basename, "fce", s_symbol])
        
        #self.s_fce_remote_prefix = os.sep.join([s_exchange_basename, "fce", s_symbol])

        self.s_fce_local_dir = s3_store.lsep.join([s_app_dir, 'tmp', s_exchange_basename, 'fce', s_symbol])
        
