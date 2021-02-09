#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/plot/SVGUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.05.2019
# Last Modified Date: 09.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, List, Any, Dict
import zipfile, io, base64
import svgutils as su
import matplotlib as plt
from cairosvg import svg2png
from IPython.display import Image
from ampel.protocol.LoggerProtocol import LoggerProtocol


class SVGUtils:

	@staticmethod
	def mplfig_to_svg_dict(
		mpl_fig, file_name: str, title: Optional[str] = None, tags: Optional[List[str]] = None,
		compress: bool = True, width: Optional[int] = None, height: Optional[int] = None,
		close: bool = True, logger: LoggerProtocol = None
	) -> Dict[str, Any]:
		"""
		:param mpl_fig: matplotlib figure
		:param tags: list of plot tags
		:param width: figure width, for example 10 inches
		:param height: figure height, for example 10 inches
		:returns: svg dict instance
		"""

		if logger:
			logger.info("Saving plot %s" % file_name)

		imgdata = io.StringIO()

		if width is not None and height is not None:
			mpl_fig.set_size_inches(width, height)

		mpl_fig.savefig(
			imgdata, format='svg', bbox_inches='tight'
		)

		if close:
			plt.pyplot.close(mpl_fig)

		ret: Dict[str, Any] = {'name': file_name}

		if tags:
			ret['tag'] = tags

		if title:
			ret['title'] = title

		if not compress:
			return {
				**ret,
				'compressed': False,
				'svg': imgdata.getvalue()
			}

		outbio = io.BytesIO()

		zf = zipfile.ZipFile(
			outbio, "a", zipfile.ZIP_DEFLATED, False
		)

		zf.writestr(
			file_name,
			imgdata.getvalue().encode('utf8')
		)

		zf.close()

		return {
			**ret,
			'compressed': True,
			'svg': outbio.getvalue()
		}


	@staticmethod
	def decompress_svg_dict(svg_dict: Dict[str, Any]) -> Dict[str, Any]:
		"""
		Modifies input dict by potentionaly decompressing compressed 'svg' value
		"""

		if not isinstance(svg_dict, dict):
			raise ValueError("Parameter svg_dict must be an instance of dict")

		if svg_dict.get('compressed'):
			bio = io.BytesIO()
			bio.write(svg_dict['svg'])
			zf = zipfile.ZipFile(bio)
			file_name = zf.namelist()[0]
			svg_dict['svg'] = str(zf.read(file_name), "utf8")
			del svg_dict['compressed']

		return svg_dict


	@staticmethod
	def rescale(svg: str, scale: float = 1.0) -> str:

		# Get SVGFigure from file
		original = su.transform.fromstring(svg)

		# Original size is represetnted as string (examle: '600px'); convert to float
		original_width = float(original.width.split('.')[0])
		original_height = float(original.height.split('.')[0])

		scaled = su.transform.SVGFigure(
			original_width * scale,
			original_height * scale
		)

		# Get the root element
		svg_st = original.getroot()

		# Scale the root element
		svg_st.scale_xy(scale, scale)

		# Add scaled svg element to figure
		scaled.append(svg_st)

		return str(scaled.to_str(), "utf-8")
		#return scaled.to_str()


	@staticmethod
	def to_png(content: str, dpi: int = 96) -> str:

		return '<img src="data:image/png;base64,' + str(
			base64.b64encode(
				Image(
					svg2png(
						bytestring=content,
						dpi=dpi
					),
				).data
			),
			"utf-8"
		) + '">'
