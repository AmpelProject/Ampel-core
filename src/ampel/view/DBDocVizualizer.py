#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/view/DBDocVizualizer.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.01.2018
# Last Modified Date: 04.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import copy, json, collections, bson, re, datetime, jdcal
from IPython.display import Markdown, display

from ampel.base.flags.TransientFlags import TransientFlags
from ampel.base.flags.PhotoFlags import PhotoFlags
from ampel.core.flags.T2RunStates import T2RunStates
from ampel.core.flags.AlDocType import AlDocType
from ampel.core.flags.FlagUtils import FlagUtils

class DBDocVizualizer:
	""" 
	""" 

	def pretty_print_db_entry(self, db_dict):
		# pylint: disable=bad-builtin
		print(
			json.dumps(
				self.translate_db_entry(db_dict), 
				sort_keys=False, 
				indent=2
			)
		)
		

	def translate_db_entry(self, db_dict):
		"""
		"""

		if 'alDocType' not in db_dict:
			return db_dict

		d = collections.OrderedDict()
			
		#al_doc_type = FlagUtils.dbflag_to_enumflag(d['alDocType'], AlDocType)
		if db_dict['alDocType'] == AlDocType.PHOTOPOINT:
			self.set_dict_first_keys(db_dict, d, "PHOTOPOINT")
			if "alFlags" in db_dict:
				d['alFlags'] = self.pretty_print_flag(
					FlagUtils.dbflag_to_enumflag(db_dict['alFlags'], PhotoFlags)
				)

			# Convert JD into gregorian date
			jdd = jdcal.jd2gcal(jdcal.MJD_0, db_dict['jd'])
			dtd = datetime.datetime(*jdd[:3]) + datetime.timedelta(days=jdd[3])
			db_dict['jd'] = dtd.strftime("%d/%m/%y - %Hh%Mm%Ss")

			self.copy_the_rest(db_dict, d)

		elif db_dict['alDocType'] == AlDocType.COMPOUND:
			self.set_dict_first_keys(db_dict, d, "COMPOUND")
			self.copy_the_rest(db_dict, d)

		elif db_dict['alDocType'] == AlDocType.TRANSIENT:
			self.set_dict_first_keys(db_dict, d, "TRANSIENT")
			if "alFlags" in db_dict:
				d['alFlags'] = self.pretty_print_flag(
					FlagUtils.dbflag_to_enumflag(db_dict['alFlags'], TransientFlags)
				)
			self.copy_the_rest(db_dict, d)
			job_ids = [] 
			for el in db_dict['jobIds']:
				job_ids.append("ObjectId(" + str(el) + ")")
			d['jobIds'] = job_ids

		elif db_dict['alDocType'] == AlDocType.T2RECORD:
			self.set_dict_first_keys(db_dict, d, "T2RECORD")
			d['channels'] = db_dict['channels']
			d['t2Unit'] = db_dict['t2Unit']
			d['runConfig'] = db_dict['runConfig']
			d['compoundId'] = db_dict['compoundId']
			self.copy_the_rest(db_dict, d)

		return d


	def set_dict_first_keys(self, d_src, d_dest, doc_type):

		if type(d_src['_id']) == bson.objectid.ObjectId:
			d_dest['_id'] = "ObjectId(" + str(d_src['_id']) + ")"
		else:
			d_dest['_id'] = d_src['_id']
		d_dest['alDocType'] = doc_type
		d_dest['tranId'] = d_src['tranId']
		

	def pretty_print_flag(self, flag):
		return str(flag).split(".")[-1]


	def markup_print_db_entry(self, db_cursor):

		json_parts = []

		for el in db_cursor:
			json_parts.append(
				self.json_to_str(
					self.translate_db_entry(el)	
				)
			)

		self.markdown_display_json(
			"".join(json_parts), fine_tune = True
		)


	def json_to_str(self, db_doc):
		"""
		"""
		return json.dumps(
			db_doc, 
			indent=True, 
			sort_keys=False
		)
		

	def markdown_display_json(self, json_str, fine_tune=True):
		"""
		"""

		if fine_tune:
			json_str = self.fine_tune_output(json_str)

		display(
			Markdown(
				"```json\n" + json_str + "\n```"
			)
		)


	def fine_tune_output(self, json_str):

 		# replace "alDocType": "T2RECORD"    with    "alDocType": T2RECORD
		for el in ["alDocType", "alFlags", "channels", "t2Compute", "runState"]:
			json_str = re.sub(el+"\": \"(.*)\"", el+"\": \\1", json_str)
  
		# replace "ObjectId(5a58ee386f21dad72c56f95e)" with ObjectId("5a58ee386f21dad72c56f95e")
		json_str = re.sub("\"ObjectId\((.*)\)\"", "ObjectId(\"\\1\")", json_str)

		# replace 
		# {
		#   "pp": 1000000          with         {"pp": 1000000}
		# }
		json_str = re.sub("{.*\n.*\"pp\": (.*)\n.*}", "\t{\"pp\": \\1}", json_str)

		# replace
		# }{  with    },
		#             {
		json_str = re.sub("}{\n", "},\n{", json_str)

		return json_str
  

	def copy_the_rest(self, d_src, d_dest):

		if "runState" in d_src:
			d_dest['runState'] = self.pretty_print_flag(
				FlagUtils.dbflag_to_enumflag([d_src['runState']], T2RunStates)
			)

		for key in d_src.keys():
			if key in d_dest:
				continue
			else:
				d_dest[key] = d_src[key]
