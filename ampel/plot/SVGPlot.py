#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/plot/SVGPlot.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.06.2019
# Last Modified Date: 09.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

# type: ignore[import]

from types import List, Optional
from ampel.type import StockId
from ampel.plot.SVGUtils import SVGUtils

class SVGPlot:

	#display_div = '<div style="display: inline-flex; justify-content: center;">'

	def __init__(
		self, content, scale: float = 1.0, title: Optional[str] = None,
		tags: Optional[List[str]] = None, stock_id: Optional[StockId] = None,
		title_left_padding: int = 0, center: bool = True
	):

		self._title = title
		self._content = content
		self._scale = scale
		self._tags = tags
		self._title_left_padding = title_left_padding
		self._center = center
		self._stock_id = stock_id


	def rescale(self, scale: float = 1.0) -> None:

		if self._scale == scale:
			return

		self._content = SVGUtils.rescale(self._content, scale)
		self._scale = scale


	def has_tag(self, tag: str) -> bool:
		return tag in self._tags


	def has_tags(self, tags: List[str]) -> bool:
		return all(el in self._tags for el in tags)


	def _repr_html_(
		self, scale: Optional[float] = None, show_title: bool = True,
		title_prefix: Optional[str] = None, show_tags: bool = False,
		padding_bottom: int = 0, png_convert: bool = False
	) -> str:
		"""
		:param scale: if None, native scaling is used
		"""

		html = "<center>" if self._center else ""

		html += '<div style="padding-bottom: %ipx">' % padding_bottom

		if show_title:
			html += '<h3 style="padding-left:%ipx">%s %s</h3>' % (
				self._title_left_padding,
				"" if title_prefix is None else title_prefix,
				self._title
			)

		if show_tags:
			html += '<h3>' + str(self._tags) + '</h3>'

		#html += SVGPlot.display_div
		html += "<div>"

		if scale is not None and isinstance(scale, (float, int)):
			if png_convert:
				html += SVGUtils.to_png(
					SVGUtils.rescale(self._content, scale),
					dpi=png_convert
				)
			else:
				html += SVGUtils.rescale(self._content, scale)
		else:
			html += SVGUtils.to_png(self._content, dpi=png_convert) if png_convert else self._content

		return html + '</div></div></center>' if self._center else '</div></div>'


	def show_html(self, **kwargs):
		"""
		:param **kwargs: see _repr_html_ arguments for details
		"""
		from IPython.display import HTML
		return HTML(
			self._repr_html_(**kwargs)
		)
