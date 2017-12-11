from enum import Flag, auto

class LogRecordFlags(Flag):
	"""
		Flags used for each log entry stored into the NoSQL database
	"""
	INFO      			 = auto()
	DEBUG      			 = auto()
	WARNING    			 = auto()
	ERROR    			 = auto()
	CRITICAL   			 = auto()

	ZTF      			 = auto()
	ASSASSIN 			 = auto()
	ICECUBE   			 = auto()

	HAS_ALERTID          = auto()
	HAS_CANDID           = auto()

	T0		             = auto()
	T1		             = auto()
	T2		             = auto()
	T3		             = auto()

	NO_CHANNEL			 = auto()
	CHANNEL_SN			 = auto()
	CHANNEL_NEUTRINO	 = auto()
	CHANNEL_RANDOM		 = auto()
	CHANNEL_OTHER1		 = auto()
	CHANNEL_OTHER2		 = auto()
	CHANNEL_OTHER3		 = auto()
	CHANNEL_OTHER4		 = auto()

	T2_SNIA_LC           = auto()
	T2_SNII_LC           = auto()
	T2_AGN               = auto()
	T2_PHOTO_Z           = auto()
	T2_PHOTO_TYPE        = auto()
