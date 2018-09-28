import logging, time

class ExtraLogFormatter(logging.Formatter):

	def format(self, record):

		extra = getattr(record, 'extra', None)

		out = [
			self.formatTime(record, datefmt=self.datefmt),
			record.filename,
			str(record.lineno),
			record.levelname,
			record.msg,
		]

		if extra:
			out.insert(-1, "[%s]" % ', '.join("%s=%s" % itm for itm in extra.items()))

		return " ".join(out)
