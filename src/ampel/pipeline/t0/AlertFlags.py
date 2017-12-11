from enum import Flag, auto

class AlertFlags(Flag):
	"""
		Flags used for T0 AmpelAlert convenience only. 
		Stored as static AmpelAlert class variable.
		Not transfered or synced with DB.
		Theses flags allow T0 filters to query
		what kind of alerts they are dealing with
	"""

	NO_FLAG				= 0

	INST_ZTF			= auto()
	INST_OTHER1			= auto()
	INST_OTHER2			= auto()
	INST_OTHER3			= auto()

	ALERT_IPAC			= auto()
	ALERT_NUGENS		= auto()
	ALERT_OTHER1		= auto()
	ALERT_OTHER2		= auto()

	PP_IPAC				= auto()
	PP_WZM				= auto()
	PP_HU				= auto()

