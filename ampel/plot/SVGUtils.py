#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/plot/SVGUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.05.2019
# Last Modified Date: 13.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import warnings
from typing import Optional, List, Dict, Any
import zipfile, io, base64
import matplotlib as plt
from cairosvg import svg2png
from IPython.display import Image
from ampel.protocol.LoggerProtocol import LoggerProtocol
from ampel.content.SVGRecord import SVGRecord
from ampel.log.AmpelLogger import AmpelLogger
from ampel.model.PlotProperties import PlotProperties
from matplotlib.figure import Figure

# catch SyntaxWarning: "is not" with a literal. Did you mean "!="?
with warnings.catch_warnings():
	warnings.filterwarnings(
		action="ignore",
		category=SyntaxWarning,
		module=r"svgutils\.transform"
	)
	import svgutils as su

class SVGUtils:


	@staticmethod
	def mplfig_to_svg_dict(
		mpl_fig, file_name: str, title: Optional[str] = None, tags: Optional[List[str]] = None,
		compress: int = 1, width: Optional[int] = None, height: Optional[int] = None,
		close: bool = True, fig_include_title: Optional[bool] = False, logger: Optional[LoggerProtocol] = None
	) -> SVGRecord:
		"""
		:param mpl_fig: matplotlib figure
		:param tags: list of plot tags
		:param compress:
			0: no compression, 'svg' value will be a string
			1: compress svg, 'svg' value will be compressed bytes (usage: store plots into db)
			2: compress svg and include uncompressed string into key 'sgv_str'
			(useful for saving plots into db and additionaly to disk for offline analysis)
		:param width: figure width, for example 10 inches
		:param height: figure height, for example 10 inches
		:returns: svg dict instance
		"""

		if logger:
			logger.info("Saving plot %s" % file_name)

		imgdata = io.StringIO()

		if width is not None and height is not None:
			mpl_fig.set_size_inches(width, height)

		if title and fig_include_title:
			mpl_fig.suptitle(title)

		mpl_fig.savefig(
			imgdata, format='svg', bbox_inches='tight'
		)

		if close:
			plt.pyplot.close(mpl_fig)

		ret: SVGRecord = {'name': file_name}

		if tags:
			ret['tag'] = tags

		if title:
			ret['title'] = title

		if compress == 0:
			ret['compressed'] = False
			ret['svg'] = imgdata.getvalue()
			return ret

		outbio = io.BytesIO()

		zf = zipfile.ZipFile(
			outbio, "a", zipfile.ZIP_DEFLATED, False
		)

		zf.writestr(
			file_name,
			imgdata.getvalue().encode('utf8')
		)

		zf.close()

		ret['compressed'] = True
		ret['svg'] = outbio.getvalue()

		if compress == 2:
			ret['svg_str'] = imgdata.getvalue()

		return ret


	@classmethod
	def mplfig_to_svg_dict1(
		cls, mpl_fig: Figure, props: PlotProperties, extra: Optional[Dict[str, Any]] = None,
		close: bool = True, logger: Optional[LoggerProtocol] = None
	) -> SVGRecord:
		"""
		:param extra: required if file_name of title in PlotProperties use a format string ("such_%s_this")
		"""

		svg_doc = cls.mplfig_to_svg_dict(
			mpl_fig,
			file_name = props.get_file_name(extra=extra),
			title = props.get_title(extra=extra),
			fig_include_title = props.fig_include_title,
			width = props.width,
			height = props.height,
			tags = props.tags,
			compress = props.get_compress(),
			logger = logger,
			close = close
		)

		if props.disk_save:
			file_name = props.get_file_name(extra=extra)
			if logger and isinstance(logger, AmpelLogger) and logger.verbose > 1:
				logger.debug("Saving %s/%s" % (props.disk_save, file_name))
			with open("%s/%s" % (props.disk_save, file_name), "w") as f:
				f.write(
					svg_doc.pop("svg_str") # type: ignore
					if props.get_compress() == 2
					else svg_doc['svg']
				)

		return svg_doc


	@staticmethod
	def decompress_svg_dict(svg_dict: SVGRecord) -> SVGRecord:
		"""
		Modifies input dict by potentionaly decompressing compressed 'svg' value
		"""

		if not isinstance(svg_dict, dict):
			raise ValueError("Parameter svg_dict must be an instance of dict")

		if svg_dict.get('compressed'):
			bio = io.BytesIO()
			bio.write(svg_dict['svg']) # type: ignore
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
