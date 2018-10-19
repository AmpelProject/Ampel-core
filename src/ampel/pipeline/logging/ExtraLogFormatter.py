import logging, time

class ExtraLogFormatter(logging.Formatter):

	def format(self, record):

		extra = getattr(record, 'extra', None)

		out = [
			self.formatTime(record, datefmt=self.datefmt),
			record.filename[:-3], # cut the '.py'
			record.levelname,
		]

		if extra:
			out.append("[%s]" % ', '.join("%s=%s" % itm for itm in extra.items()))

		if record.msg:
			return "<%s>\n%s" % (" ".join(out), record.msg)
		else:
			return "<%s>" % " ".join(out)
