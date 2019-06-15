#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ZTF/Ampel/src/ampel/view/plot/SVGBrowser.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.06.2019
# Last Modified Date: 13.06.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.view.plot.SVGCollection import SVGCollection
from ampel.view.plot.SVGLoader import SVGLoader
from IPython.display import HTML, display
from collections import defaultdict
import random

class SVGBrowser:
	"""
	"""

	def __init__(self, svg_loader, scale=None, show_col_title=True, show_svg_titles=True):
		""" """

		self._scale = scale
		self._svg_loader = svg_loader
		self._show_col_title = show_col_title
		self._show_svg_titles = show_svg_titles


	def show_multi_window(self):
		""" 
		Shows all plots for one transient in one window
		"""
		pass


	def show_single_window(
		self, scale=None, append_tran_name=True, 
		win_title="Ampel plots", tran_ids=None, 
		png_convert=False, multiproc=0
	):
		""" 
		:param bool append_tran_name: appends transient name to plot titles
		"""

		win_id = SVGBrowser._new_window(win_title)

		if multiproc:

			from concurrent.futures import ProcessPoolExecutor
			futures = []

			with ProcessPoolExecutor(max_workers=multiproc) as executor:

				for tran_id in self._svg_loader._plots.keys():
					futures.append(
						executor.submit(
							get_html, 
							tran_id, 
							self._svg_loader._data_query,
							self._svg_loader._t2_query,
							scale,
							png_convert
						)
					)

				for future in futures:
					SVGBrowser._write_to_window(
						win_id, future.result(), 
						'Adding %s info to %s<br>' % (tran_id, win_title)
					)
		else:

			for tran_id in self._svg_loader._plots:

				if tran_ids:
					if self._svg_loader._plots[tran_id] not in tran_ids:
						continue

				SVGBrowser._write_to_window(
					win_id, 
					self._svg_loader._plots[tran_id]._repr_html_(
						scale=scale, 
						title_prefix=tran_id, 
						png_convert=png_convert
					), 
					'Adding %s info to %s<br>' % (tran_id, win_title)
				)

		display(HTML("Done"))


	@staticmethod
	def _new_window(title):
		""" """

		win_id = "win_" + str(random.randint(0, 100000))

		display(
			HTML(
				'<script type="text/Javascript"> \
					var %s=window.open("", "%s"); \
					win.document.title="%s"; \
				 </script>' % (win_id, title, title)
			)
		)

		return win_id


	@staticmethod
	def _write_to_window(js_var_name, html_content, feedback=""):
		""" """
		display(
			HTML(
				'%s<script type="text/Javascript"> \
					%s.document.body.innerHTML += \'%s\';' \
				'</script>' % (
					feedback, 
					js_var_name, 
					html_content.replace("\n","\\")
				)
			)
		)


def get_html(tran_id, data_query, t2_query, scale=1.0, png_convert=False):
	""" 
	"""

	svg_loader = SVGLoader()
	svg_loader._data_query = data_query
	svg_loader._t2_query = t2_query
	svg_loader.load_plots()

	return svg_loader._plots[tran_id]._repr_html_(
		scale=scale, 
		title_prefix=tran_id, 
		png_convert=png_convert
	)
