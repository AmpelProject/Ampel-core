import json

class ZIJSONLoader:
	"""
		ZI is a shortcut for ZTF IPAC.

		This class is responsible for:
			* Loading IPAC generated ZTF Alerts in JSON format 
			* Possibly filtering 'prv_candidates' photopoints 

		The static method get_flat_pps_list_from_json() returns the transient id 
		and a list dictionaries representing the associated photopoints
	"""

	@staticmethod
	def get_flat_pps_list_from_json(file_path):
	
		import json
		with open(file_path, "r") as data_file:
			json_dict = json.load(data_file)

		json_dict['prv_candidates'].insert(0, json_dict['candidate'])
		return json_dict['alertId'], json_dict['prv_candidates']
