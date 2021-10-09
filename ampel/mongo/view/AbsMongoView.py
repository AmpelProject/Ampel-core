#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/view/MongoChannelView.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.03.2021
# Last Modified Date: 26.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Any, Dict
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelBaseModel import AmpelBaseModel


class AbsMongoView(AmpelABC, AmpelBaseModel, abstract=True):

	@abstractmethod
	def stock(self) -> List[Dict[str, Any]]:
		...

	@abstractmethod
	def t0(self) -> List[Dict[str, Any]]:
		...

	@abstractmethod
	def t1(self) -> List[Dict[str, Any]]:
		...

	@abstractmethod
	def t2(self) -> List[Dict[str, Any]]:
		...

	@abstractmethod
	def t3(self) -> List[Dict[str, Any]]:
		...

	@abstractmethod
	def get_meta_cases(self, arg: str) -> List[Dict[str, Any]]:
		...

	def conform_meta(self) -> Dict[str, Any]:
		""" To be overriden if need be """

		return {
			'$filter': {
				'input': {
					"$map": {
						"input": "$meta",
						'as': "metad",
						"in": {
							"$mergeObjects": [
								"$$metad",
								{
									"activity": {
										'$filter': {
											'input': {
												"$map": {
													"input": "$$metad.activity",
													'as': "activityd",
													"in": {
														"$switch": {
															"branches": [
																{
																	# Handles meta activities without channel
																	'case': {'$lte': ["$$activityd.channel", None]},
																	'then': "$$activityd"
																},
																*self.get_meta_cases("$$activityd")
															],
															"default": False
														}
													}
												}
											},
											"as": "ad",
											"cond": "$$ad"
										}
									}
								}
							]
						}
					}
				},
				"as": "md",
				"cond": "$$md.activity"
			}
		}
