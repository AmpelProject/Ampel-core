from ampel.pipeline.t0.AlertFlags import AlertFlags
from werkzeug.datastructures import ImmutableDict, ImmutableList


class TransientTimeSerie:
	"""
		ALPHA CLASS. do not use
	"""

	def __init__(self, mongo_client, tran_id, param_id, compound_id):

		"""
		self.db = mongo_client["Ampel"]
		self.col_pps = self.db["photopoints"]
		self.col_tran = self.db["transients"]
		self.col_t2 = self.db["t2"]

		self.tran_id = tran_id
		self.param_id = param_id
		self.compound_id = compound_id

	
		t2doc = self.col_t2.find(
			{	"t2Module": t2_module.value, 
				"paramId": paramId, 
				"compoundId": compoundId,
			}
		)
		"""
