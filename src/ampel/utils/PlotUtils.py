#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel/src/ampel/utils/PlotUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.05.2019
# Last Modified Date: 17.05.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import zipfile, io
import matplotlib as plt

class PlotUtils:
	"""
	"""

	@staticmethod
	def mplfig_to_svg_dict(
		mpl_fig, file_name, title=None, tags=None,
		compress=True, width=None, height=None, close=True
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
