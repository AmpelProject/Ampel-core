#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/filters/BasicFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.01.2018
# Last Modified Date: 08.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from ampel.abstract.AbsAlertFilter import AbsAlertFilter
from extcats import CatalogQuery

from pymongo import MongoClient
from operator import not_
from numpy import array, mean


class BasicCatalogFilter(AbsAlertFilter):

    version = 0.1

    def __init__(self, on_match_t2_units, base_config=None, run_config=None, logger=None):
        """
            base_config taken from: Ampel/config/hu/t0_filters/config.json
            run_config from: Ampel/config/hu/channels/config.json
        """

        self.on_match_default_t2_units = on_match_t2_units

        if run_config is None or type(run_config) is not dict:
            raise ValueError("Method argument must be a dict instance")

        # init mongo client
        dbclient = MongoClient(host = base_config['mongodbHost'], port = base_config['mongodbPort'])
        
        # int catalogquery object
        self.cat_query = CatalogQuery.CatalogQuery(
            cat_name = run_config['catName'],
            ra_key = run_config['catRAKey'], 
            dec_key = run_config['catDecKey'], 
            coll_name = base_config['catSrcCollName'], 
            dbclient = dbclient, 
            logger =  logger)
        
        # parameters for the query
        self.rs_arcsec = run_config['rsArcsec']
        self.alert_pos_type = run_config['alertPosType']
        self.search_method = run_config['searchMethod']
        
        # either accept or reject alert if a match is found
        self.reject_on_match = run_config['rejectOnMatch']
        
        logger.info(
            "Initialized BasicCatalogFilter. Alerts with matches in %s (rs = %.2f arcsec) will be %s"%
                (run_config['catName'],
                run_config['rsArcsec'],
                "dropped"*self.reject_on_match + "accepted"*(not self.reject_on_match)))


    def apply(self, ampel_alert):
        """
        Doc will follow
        """
        
        if self.alert_pos_type == "av":
            ras = array(ampel_alert.get_values('ra'))
            decs = array(ampel_alert.get_values('dec'))
            ra, dec = mean(ras), mean(decs)
        elif self.alert_pos_type == "latest":
            ras, dec = ampel_alert.get_values('ra')[-1], ampel_alert.get_values('dec')[-1]
        else:
            raise ValueError("value %s for BasicCatalogFilter param alertPosType not recognized.")

        if self.cat_query.binaryserach(ra, dec, rs_arcsec = self.rs_arcsec, method = self.search_method):
            if self.reject_on_match:
                return None
            else:
                return self.on_match_default_t2_units
                
