import hashlib

class HashUtils():

	def __init__(self) :
		"""
		"""
		self.version = ""
		self.primary_feed = ""
		self.additional_feed = ""

	def set_version(self, arg):
		""" """
		self.version = arg

	def set_photopoints(self, d):
		""" later """

	def set_alert(self, alert):
		"""  """
		self.primary_feed = str(alert['candid'])
		self.primary_feed += "".join([str(el['candid']) for el in sorted(alert['prv_candidates'], key=lambda dd: dd['jd'])])

	def set_sorted_pps_list(self, pps, keys=["candid", "wz"]):
		return "".join(["".join([str(el[key]) for key in keys]) for el in pps])

	def get_hash(self):
		return hashlib.md5((self.version + self.primary_feed + self.additional_feed).encode('utf-8')).digest()

	def get_hash_str(self):
		return self.get_hash().hexdigest()
