import logging
from ampel.pipeline import DBLoggingHandler
from ampel import PhotoPointFlags


class AmpelPhotoPoint:
	"""
	"""

	@staticmethod
	def nameitlater():

		d = {
    		"global" : {
        		"pp_dicts" : {
        		    "INST_ZTF|PP_IPAC": {
            		    "alert_id" : "alertid",
            		    "subres_id" : "candid",
            		    "pp_jd" : "jd",
            		    "filter_id" : "fid"
					},
        		    "INST_ZTF|PP_HUMBOLDT": {
            		    "alert_id" : "alertid",
            		    "subres_id" : "candid",
            		    "pp_jd" : "jd",
            		    "filter_id" : "fid"
					}
				}
			}
		}

		for pp_dict in d["global"]["pp_dicts"]:

			int_flag_value = 0

			for flag in pp_dict["alflags"].split("|"):
				int_flag_value += PhotoPointFlags[flag].value

			ppf = PhotoPointFlags(int_flag_value)
