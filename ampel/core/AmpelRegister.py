#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/AmpelRegister.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.05.2020
# Last Modified Date: 26.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import bson
from time import time
from os.path import isdir, isfile
from pathlib import Path
from struct import calcsize
from typing import BinaryIO, Optional, Literal, Dict, Any, List, Union, Tuple, TypedDict

from ampel.log.AmpelLogger import AmpelLogger, VERBOSE
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.util.mappings import build_unsafe_dict_id
from ampel.util.register import read_header, write_header, \
	get_inner_file_handle, get_outer_file_handle, rescale_header


class HeaderInfo(TypedDict):
	size: int
	len: int
	payload: Dict[str, Any]


class AmpelRegister(AmpelBaseModel):
	""" # noqa: E101

	General notes:
	--------------

	An ampel register file is made of three parts: signature, header and content

	1) The file signature is encoded in the first 11 bytes.
	The signature contains the header's length and size (3 bytes each)

	2) A BSON encoded dict is saved in the next x bytes, refered to as the file's header.
	The header can be updated independently of the register's content.
	The size of the header is customizable during file creation.

	3) The register's content is usually a compressed structure (zip, bzip or xz)
	which stores information in binary format. Compression can be turned off if so whished.
	(The size of a register containing 10 millions alert ids [8 bytes] and filter
	return code [1 byte] can be reduced from ~90MB to ~50MB using gzip with default settings).

	Properties:
	----------
	- Registers can be re-opened and appended
	- Header content can be accessed or updated independently of the register's content.
	- Header updates are fast if enough space was reserved for updates in the first place.
	- Header size can be increased afterwards at the cost of having to rewrite the entire file once.
	  This happens automatically when needed.
	- Logging read and write access to the register is supported
	- File rotation is supported (based on max content length or max number of run ids)

	Note: the module ampel.util.register contains tools to manually change ampel registers, such as:
	`get_header_content`, `open_file_and_write_header` and `rescale_header`

	Target file:
	------------

	:param path_base: the base folder path where to create/read register files.
	:param path_extra: an optional additional folder path (or paths) to be appended to `path_base`.
	:param full_path: when provided, all path building options will be ignored and a new
	file handle will be opened using the provided full_path
	:param file_handle: if you provide a file handle to a register file (parameter file handle),
	it will be used and all path options will be ignored.

	:param file_rotate: this class is able to rotate files based on a maximum number of blocks.
	Note1: file rotation occurs during the opening of registers, meaning that once a register is opened,
	no check is performed (a register can thus grow beyond the defined limit as long as a process keeps it open)
	Note2: the current file rotation number is encoded in the header. If the current rotation number is 10 and
	you move the rotated file to another folder, the next rotation will create ampel_register.bin.gz.11 nonetheless.

	Header:
	-------

	Limitations:
	- The maximum space reservable for the header is 2**24 bytes, i.e ~16MB.
	- integers cannot exceed 2**63 bits. Should you need to save bigger numbers,
	please use the methods bindata_to_int, int_to_bindata from module ampel.util.bson

	:param new_header_size: either None, or an integer or a string (please read the note above).
	- None or 0: the header block size will equal the header encoded length. Choose this option if
	the header is not meant to be updated later. Otherwise, updates will only be possible if the header size
	does not grow (note that there is a margin allowed since header exceeding the limit are automatically
	compressed using zlib and written to disk if the size condition is then fullfilled).
	- integer number: the header will be allocated the specified number of bytes (for example 4096).
	- a string: refers to a 'header margin' and must start with the character '+'.
	This option can save space in some circumstances. The header space allocated for the header will equal
	the length of the initial header (including all provided options such as `header_extra`) to which
	the specified header margin will be added. For example, '+1024' means that 1024 bytes will be
	allocated additionally to the initial header lengths for future updates. If the initial header length
	is 100 bytes, then a header block of 1124 bytes will be created.

	:param header_extra: any extra to be included in the header under the key 'extra' (must be bson encodable)
	:param header_extra_base: any extra to be included in the header at root depth (must be bson encodable)
	:param header_update_anyway: if no update to the register is made, the default setting is that the
	header is not updated. This settings forces header updates. For example, you might want
	to save all run ids into the header whether or not they changed the content of the register.

	:param header_log_accesses: if True, timestamps will be recorded each time the register is opened/closed
	along with the amount of new blocks appended to the register.
	Note 1: parameter `new_header_size` must be set when using this value.
	Note 2: see docstring of paramter `header_update_anyway` that affects the behavior of this parameter.
	In the following example, `header_log_accesses` is responsible for creating/updating the key 'updated':

    "ts": {
        "created": 1590506868.3880599,
        "updated": [
            [1590506868.3880599, 1590509029.389, 1200],
            [1590507152.079873, 1590507295.080478, 2300]
       ]
	}

	Errors:
	-------

	:raises: ValueError, FileNotFoundError
	- if neither `file_handle`, `full_path`, `path_base` exist.
	- if read access to a non-existing file is requested
	- if the target file is not a register file (existing empty files are ok)
	- if the 'struct' parameter of sub-class of this class differs with the values registered
	  in the file's header (this behavior can be deactivated from parameter `on_exist_check`)
	- during file rotation if the target file already exists

	Possible setups:
	----------------
	Register files are named after run_id:
	<path>/<channel>/121.bin.gz
	<path>/<channel>/122.bin.gz
	<path>/<channel>/123.bin.gz
	...

	The register header is updated before each file is closed to save min and max alert IDs
	and min and max stock ids. That way, a potential query for a given alert rejection could
	go through all register files, parse only the header and check if min_alert < target_alert < max_alert,
	and will by that not have to go through the file if the condition is not fulfilled.

	Pro:
	- Avoids any potential concurency issue
	- Fast query
	Cons:
	- Can generate numerous files (whichs should not be a pblm for any modern file system)

	2) register file are named after channel id:
	<path>/AMPEL_CHANNEL1.register.gz
	<path>/AMPEL_CHANNEL2.register.gz
	...

	Log rotate can be performed based on file size (file_rotate='blocks')

	Pro:
	- Less files ?
	Cons:
	- beware: re-run should not use this scheme, as concurent updates to a register are not supported!
	- slower query (bigger files)
	"""

	struct: str
	verbose: int = 0
	logger: AmpelLogger

	# File path options
	path_base: Optional[str]
	path_extra: Optional[List[str]] # save files in <path_base>/<path_extra(s)>/<file>
	file_prefix: Optional[str]
	path_full: Optional[str] # Ignore all previous options and use a fixed file path

	file_rotate: Optional[Dict[Literal['blocks'], int]]

	# Option to provide an existing file handle
	file_handle: Optional[BinaryIO]

	# Compression options
	compression: Optional[Literal['gz', 'bz2', 'xz']] = 'gz'
	compress_level: Optional[int]

	# General header options
	new_header_size: Optional[Union[int, str]]

	header_log_accesses: bool = False
	header_count_blocks: bool = True
	header_extra: Optional[Dict[str, Any]]
	header_extra_base: Optional[Dict[str, Any]]
	header_update_anyway: bool = False

	# New header options
	header_creation_size: Optional[int]

	# Which header key to check if file already exists
	on_exist_check: Optional[List[Union[str, Tuple[str, str]]]] = ['struct']
	on_exist_strict_check: bool = False

	def __init__(self, autoload: bool = True, **kwargs) -> None:
		""" See class docstring """

		super().__init__(**kwargs)

		if not hasattr(self, 'struct'):
			raise ValueError("Sub-classes of AmpelRegister must define static field 'struct'")

		if self.header_log_accesses and not self.new_header_size:
			raise ValueError("Parameter 'new_header_size' is required when using 'header_log_accesses'")

		if autoload:
			self.load()


	def load(self) -> None:

		if self.file_handle:
			hinfo = read_header(self.file_handle, self.logger if self.verbose > 1 else None)
			self._outer_fh = self.file_handle
		else:
			f_path = self.get_file_path()
			hinfo, self._outer_fh = get_outer_file_handle(
				f_path, write=True, logger=self.logger if self.verbose > 1 else None
			)

			if self._outer_fh is None:
				raise ValueError(f"{f_path}: cannot get file handle")

		# File exists (and is not empty)
		if hinfo:

			if self.verbose > 1:
				self.logger.debug("Header loaded", extra=hinfo['payload'])

			if self.on_exist_check:
				self.check_header(hinfo['payload'])

			if self.file_rotate and self.check_rotate(hinfo['payload']):
				self._outer_fh = self.rotate_file(self._outer_fh, hinfo['payload'])
				hinfo = None

			else:
				self.header = hinfo

				# Update file access
				if self.header_log_accesses:
					self.register_file_access()

				# Hook for sub-classes
				self.onload_update_header()


		if hinfo is None:

			if self.verbose > 1:
				self.logger.debug("Generating new header")

			header_bytes = self.gen_new_header()
			write_header(
				self._outer_fh, header=header_bytes, hsize=self.header['size'],
				logger=self.logger if self.verbose else None
			)

		self._inner_fh = get_inner_file_handle(
			self._outer_fh, write=True,
			logger=self.logger if self.verbose > 0 else None
		)

		# Non-compressed file returns the EOF position when opened in mode 'ab'
		# compressed file return 0
		if self.compression is None:
			self._ftell = self._inner_fh.tell()

		self.header_sig = build_unsafe_dict_id(self.header['payload'])


	def check_rotate(self, header: Dict[str, Any]) -> bool:
		""" override if needed """

		if not self.file_rotate:
			return False

		if 'blocks' in self.file_rotate:

			if header['blocks'] > self.file_rotate['blocks']:
				return True

			if self.verbose > 1:
				self.logger.debug("File rotation trigger not reached")

			return False

		self.logger.warn(f"Unknown 'file_rotate' value: {self.file_rotate}")

		return False


	def rotate_file(self, fh: BinaryIO, header: Dict[str, Any]) -> BinaryIO:

		fh.close()
		self.rotated = header.get('rotate', 0) + 1

		from os import rename
		rotated_file_path = f"{fh.name}.{self.rotated}"

		# we might handle this rather than raising an error in the future
		if isfile(rotated_file_path):
			raise ValueError(f"File rotation failure: {rotated_file_path} already exists")

		rename(fh.name, rotated_file_path)

		if self.verbose > 0:
			self.logger.info(f"Current register rotated into {rotated_file_path}")

		return get_outer_file_handle(
			fh.name, write=True, logger=self.logger if self.verbose > 1 else None
		)[1]


	def onload_update_header(self) -> None:
		"""
		Override if you need to update the header of an existing register.
		Ex: BaseAlertRegister adds the current run id
		"""
		pass


	def get_file_path(self) -> str:
		""" :raise: errors if sub-directories cannot be created """

		if self.path_full:
			outdir = self.path_full
		else:
			if not self.path_base:
				raise ValueError("Parameter path_base is not set")

			outdir = self.path_base
			if self.path_extra:
				outdir += '/' + '/'.join(self.path_extra)

		if not isdir(outdir):
			Path(outdir).mkdir(parents=True)

		return f"{outdir}/{self.get_file_name()}"


	def get_file_name(self) -> str:
		""" override if needed """
		return '.'.join([
			self.file_prefix or 'ampel_register',
			f'bin.{self.compression}' if self.compression else 'bin'
		])


	def check_header(self, header: Dict[str, Any]) -> None:
		"""
		:raises: ValueError is raised on mismatch between this instance value
		and the header value for the provided key
		"""

		for el in self.on_exist_check: # type: ignore[union-attr]

			if isinstance(el, tuple):
				self_key = el[0]
				hdr_key = el[1]
			else:
				self_key = hdr_key = el

			if not hasattr(self, self_key):
				raise ValueError(f"Variable {self_key} is missing, check your config")

			if hdr_key not in header:
				if self.on_exist_strict_check:
					raise ValueError(f"Attribute {hdr_key} missing in header")
				continue

			if header[hdr_key] != getattr(self, self_key):
				raise ValueError(
					f"{self.get_file_path()}: '{self_key}' mismatch: [{self.__class__.__name__}] "
					f"'{getattr(self, self_key)}' != '{header[hdr_key]}' [File header]"
				)


	def register_file_access(self,
		header: Optional[Dict[str, Any]] = None,
		use_this_time: Optional[float] = None,
		new_blocks: Optional[int] = None,
	) -> None:
		"""
		:param header: use provided header rather than self.header['payload']
		:param use_this_time: use provided time rather than time.time()
		:param new_blocks: None (default) when register is opened, 0 or an integer when register is closed
		"""
		hdr = header if header else self.header['payload']
		now = use_this_time if use_this_time else time()

		# File opened
		if new_blocks is None:
			if 'updated' in hdr['ts']:
				hdr['ts']['updated'].append([now, 0., 0])
			else: # new file
				hdr['ts']['updated'] = [[now, 0., 0]]
			return

		l = hdr['ts']['updated'][-1]
		l[1] = now
		l[2] = new_blocks

	def gen_new_header(self) -> bytes:
		"""
		Creates a new header and create instance variable self.header to reference it.
		:returns: bson encoded bytes representing the generated header
		:raises: ValueError if the generated header size exceeds user-provided bound parameters
		"""

		now = time()
		hdr: Dict[str, Any] = {
			'struct': self.struct,
			'ts': {'created': now}
		}

		if self.header_log_accesses:
			self.register_file_access(header=hdr, use_this_time=now)

		if self.header_count_blocks:
			hdr['blocks'] = 0

		if self.header_extra:
			hdr['extra'] = self.header_extra

		if self.header_extra_base:
			hdr = {**self.header_extra_base, **hdr}

		if hasattr(self, 'rotated'):
			hdr['rotate'] = self.rotated
		elif self.file_rotate:
			hdr['rotate'] = 0

		hdr_bytes = bson.encode(hdr)
		hlen = len(hdr_bytes)

		if self.new_header_size:
			if isinstance(self.new_header_size, int):
				hsize = self.new_header_size
			elif isinstance(self.new_header_size, str):
				hsize = hlen + int(self.new_header_size[1:])
			else:
				raise ValueError("Parameter new_header_size is invalid")
		else:
			hsize = hlen

		if hlen > hsize:
			raise ValueError(
				f"Header too long ({hlen} > hsize), please check the "
				f"parameters impacting new header sizes"
			)

		self.header = HeaderInfo(size=hsize, len=hlen, payload=hdr)
		return hdr_bytes


	def __del__(self):
		""" method called when class is destroyed """
		if getattr(self, '_outer_fh', None):
			if self.verbose > 1:
				self.logger.debug("Destroying ampel register instance")
			self.close()


	def close(self, close_outer_fh: bool = True, update_header: bool = True):
		"""
		:param close_outer_fh: whether principal file handle should be closed
		:param update_header: possible overidde of default settings
		(aimed for admins working with command line)
		"""

		file_updated = 0

		# Important: zip file handle should be closed before header is updated
		if hasattr(self, '_inner_fh'):

			self._inner_fh.flush()
			if self.compression:
				file_updated = self._inner_fh.tell()
			else:
				file_updated = self._inner_fh.tell() - self._ftell

			# When no compression is used, inner_fh is the outer_fh
			if self._inner_fh != self._outer_fh:
				if self.verbose > 1:
					self.logger.debug("Closing inner file")
				self._inner_fh.close()

			self._inner_fh = None # type: ignore

		if not self._outer_fh or self._outer_fh.closed:
			self.logger.info("File handle already closed")
			return
		else:
			if self.verbose:
				self.logger.log(VERBOSE, f"Closing {self.get_file_path()}")

		if hasattr(self, 'header') and update_header:

			if file_updated or self.header_update_anyway:

				if self.header_count_blocks or self.header_log_accesses:

					new_blocks = int(file_updated / calcsize(self.struct))

					if self.header_count_blocks:
						self.header['payload']['blocks'] += new_blocks

					if self.header_log_accesses:
						self.register_file_access(new_blocks=new_blocks)

				if (
					self.header_log_accesses or self.header_count_blocks or
					build_unsafe_dict_id(self.header['payload']) != self.header_sig
				):

					if self.verbose:
						self.logger.log(VERBOSE, "Header has changed, triggering update")

					try:
						write_header(
							self._outer_fh, header=self.header['payload'], hsize=self.header['size'],
							flush=False, logger=self.logger if self.verbose else None
						)
					except ValueError:
						self.logger.warn("Header still too long, rescaling it")
						self._outer_fh.flush()
						self._outer_fh.close()
						rescale_header(
							self._outer_fh.name, new_size = self.header['size'] * 2, remove_old_file = True,
							header = self.header['payload']
							)
						self._outer_fh = None # type: ignore[assignment]

				elif self.verbose > 1:
					self.logger.debug("Header was not updated")

			elif self.verbose > 1:
				self.logger.debug("File was not updated, skipping potential header updates")

		elif self.verbose > 1:
			self.logger.debug("Header update is disabled")

		if self._outer_fh:
			self._outer_fh.flush()

		if close_outer_fh:
			if self._outer_fh:
				self._outer_fh.close()
			self._outer_fh = None # type: ignore[assignment]
