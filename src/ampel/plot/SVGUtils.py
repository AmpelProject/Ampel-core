#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/view/plot/SVGUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.05.2019
# Last Modified Date: 21.05.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import zipfile, io
import matplotlib as plt
import svgutils as su
from IPython.display import Image
from cairosvg import svg2png
import base64

class SVGUtils:
	"""
	"""

	@staticmethod
	def mplfig_to_svg_dict(
		mpl_fig, file_name, title=None, tags=None,
		compress=True, width=None, height=None, 
		close=True, logger=None
	):
		"""
		:param mpl_fig: matplotlib figure
		:param str file_name: filename 
		:param str title: title 
		:param List[str] tags: list of plot tags 
		:param int width: figure width, for example 10 inches
		:param int height: figure height, for example 10 inches
		:returns: dict
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

		ret = {'name': file_name}

		if tags:
			ret['tags'] = tags

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
	def load_svg_dict(svg_dict):
		"""
		:param dict svg_dict:
		:returns: dict
		"""

		if not isinstance(svg_dict, dict):
			raise ValueError("Parameter svg_dict must be an instance of dict")

		if svg_dict['compressed']:
			bio = io.BytesIO()
			bio.write(svg_dict['svg'])
			zf = zipfile.ZipFile(bio)
			file_name = zf.namelist()[0]
			svg_dict['svg'] = str(zf.read(file_name), "utf8")

		del svg_dict['compressed']

		return svg_dict


	@staticmethod
	def rescale(svg, scale=1.0):
    
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
		svg = original.getroot()

		# Scale the root element
		svg.scale_xy(scale, scale)

		# Add scaled svg element to figure
		scaled.append(svg)
		
		return str(scaled.to_str(), "utf-8")
		#return scaled.to_str()


	@staticmethod
	def to_png_img(content, dpi=96):
		""" """
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
