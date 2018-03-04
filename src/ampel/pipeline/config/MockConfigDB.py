import json, mongomock
from ampel.pipeline.logging.LoggingUtils import LoggingUtils

class MockConfigDB:

	config_db_collection = [
		'channels', 
		'global', 
		't1', 
		't2_run_settings', 
		't2_units', 
		't3_jobs', 
		't3_run_settings', 
		't3_units'
	]

	def __init__(self, folder, db_name="Ampel_config", logger=None):

		self.logger = LoggingUtils.get_logger() if logger is None else logger
		mc = mongomock.MongoClient()
		self.db = mc[db_name]
	
		# Loop through config collections
		for col in MockConfigDB.config_db_collection:

			# Generate file path
			filepath = "%s/%s/config.json" % (folder,col)
			self.logger.info("Importing db docs from %s" % filepath)

			# Open exported json file
			with open(filepath, "r") as data_file:

				jsarray = json.load(data_file)

				# Remove $oid if present (new _id will be generated on insert)
				for el in jsarray:
					if type(el['_id']) is dict:
						del el['_id']

				# Inserting an empty json doc raises an exception
				if len(jsarray) == 0:
					continue

				# Insert all docs
				self.db[col].insert_many(jsarray)


	def get_config_db(self):
		return self.db
