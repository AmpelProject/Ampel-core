class ReadOnlyDict(dict):
	def __readonly__(self, *args, **kwargs):
		raise RuntimeError("Cannot modify ReadOnlyDict")
	__setitem__ = __readonly__
	__delitem__ = __readonly__
	pop = __readonly__
	popitem = __readonly__
	clear = __readonly__
	update = __readonly__
	setdefault = __readonly__
	del __readonly__
