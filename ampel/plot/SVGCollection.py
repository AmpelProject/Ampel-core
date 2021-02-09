#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/plot/SVGCollection.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.06.2019
# Last Modified Date: 09.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

# type: ignore[import]

from typing import Dict, Any, Optional, List
from ampel.plot.SVGUtils import SVGUtils
from ampel.plot.SVGPlot import SVGPlot

class SVGCollection:

	def __init__(self,
		title: str = None, scale: float = 1.0,
		inter_padding: int = 100, center: bool = True
	):
		"""
		:param title: title of this collection
		:param scale: scale factor for all SVGs (default: 1.0)
		:param inter_padding: sets padding in px between plots of this collection
		"""
		self._svgs = []
		self._col_title = title
		self._scale = scale
		self._inter_padding = inter_padding
		self._center = center


	def rescale(self, scale: float = 1.0) -> None:
		"""
		:param float scale: scale factor for all SVGs (default: 1.0)
		"""
		if self._scale == scale:
			return
		for el in self._svgs:
			el._content = SVGUtils.rescale(el._content, scale)
		self._scale = scale


	def set_inter_padding(self, inter_padding: int) -> None:
		"""
		Sets padding in px between plots of this collection
		"""
		self._inter_padding = inter_padding


	def add_svg_plot(self, svgp: SVGPlot) -> None:

		if not isinstance(svgp, SVGPlot):
			raise ValueError("Instance of ampel.plot.SVGPlot expected")

		self._svgs.append(svgp)


	def add_svg_dict(self, svg_dict: Dict[str, Any], title_left_padding: int = 0) -> None:
		"""
		:param Dict svg_dict:
		:param int title_left_padding:
		"""
		self._svgs.append(
			SVGPlot(
				content = svg_dict['svg'],
				title = svg_dict['title'],
				tags = svg_dict['tag'],
				title_left_padding = title_left_padding
			)
		)


	def add_raw_db_dict(self, svg_dict: Dict[str, Any]) -> None:
		"""
		:param Dict svg_dict: raw svg dict loaded from DB
		"""
		self.add_svg_dict(
			SVGUtils.decompress_svg_dict(svg_dict)
		)


	def get_svgs(self, tag: Optional[str] = None, tags: Optional[List[str]] = None) -> List[SVGPlot]:

		if tag:
			return [svg for svg in self._svgs if svg.has_tag(tag)]
		if tags:
			return [svg for svg in self._svgs if svg.has_tags(tag)]

		return self._svgs


	def _repr_html_(
		self, scale: Optional[float] = None, show_col_title: bool = True,
		title_prefix: Optional[str] = None, show_svg_titles: bool = True,
		hide_if_empty: bool = True, png_convert: bool = False,
		flexbox_wrap: bool = True
	) -> str:
		"""
		:param scale: if None, native scaling is used
		"""

		if hide_if_empty and not self._svgs:
			return None

		html = "<center>" if self._center else ""
		#html += '<hr style="width:100%; border: 2px solid;"/>'

		if show_col_title and self._col_title:
			html += '<h1 style="color: darkred">' + self._col_title + '</h1>'

		if flexbox_wrap:
			html += '<div style="\
				text-align:center; \
				display: flex; \
				flex-direction: row; \
				flex-wrap: wrap; \
				justify-content: center">'

		for svg in self._svgs:
			html += svg._repr_html_(
				scale = scale,
				show_title = show_svg_titles,
				title_prefix = title_prefix,
				padding_bottom = self._inter_padding,
				png_convert = png_convert
			)

		if flexbox_wrap:
			return html + "</div></center>" if self._center else html + "</div>"
		else:
			return html + "</center>" if self._center else html


	def show_html(self, **kwargs):
		"""
		:param **kwargs: see _repr_html_ arguments for details
		"""
		from IPython.display import HTML
		return HTML(
			self._repr_html_(**kwargs)
		)
