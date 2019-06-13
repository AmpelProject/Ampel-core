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
from IPython.display import HTML, display
from collections import defaultdict

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
		png_convert=False
	):
		""" 
		:param bool append_tran_name: appends transient name to plot titles
		"""

		html = ""

		for tran_name in self._svg_loader._plots:

			if tran_ids:
				if self._svg_loader._plots[tran_name] not in tran_ids:
					continue

			html += self._svg_loader._plots[tran_name]._repr_html_(
				scale=scale, title_prefix=tran_name, png_convert=png_convert
			)

		s  = '<script type="text/Javascript">'
		s += 'var win = window.open("", "' + win_title + '");'
		s += 'win.document.body.innerHTML = \'' + html.replace("\n",'\\') + '\';'
		s += 'win.document.title = \'' + win_title + '\';'
		s += '</script>'

		display(HTML(s))
