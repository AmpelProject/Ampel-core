#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/util/register.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 24.05.2020
# Last Modified Date: 24.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

"""
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
	See methods `get_header_content` and `open_file_and_write_header` (use at your own risk)
- Header updates are fast if enough space was reserved for updates in the first place.
- Header size can be increased afterwards at the cost of having to rewrite the entire file once (function `rescale_header`)

Known class making use of this module: `ampel.core.AmpelRegister.AmpelRegister` and sub-classes
"""

import bson, json, sys
from errno import ENOENT
from struct import iter_unpack, calcsize
from os import path, strerror
from zlib import compress, decompress
if sys.version_info.minor > 8:
	from typing import TypedDict
else:
	from typing_extensions import TypedDict
from typing import BinaryIO, Optional, Dict, Any, List, Union, Tuple, Generator, Callable
from ampel.log.AmpelLogger import AmpelLogger, VERBOSE

ampel_magic_bytes = bytes([97, 109, 112, 101, 108])


class HeaderInfo(TypedDict):
	size: int     # max 16 MB
	len: int      # <= size
	payload: Dict[str, Any]


def get_outer_file_handle(
	file_path: str, write: bool = False, logger: Optional[AmpelLogger] = None,
) -> Tuple[Optional[HeaderInfo], BinaryIO]:
	"""
	If file exists, offset file handle along with decoded header dict is returned.
	Otherwise, an untouched file handle is returned (wihtout offset)  without header info.
	:raise: FileNotFoundError if write is False and file does not exist
	"""

	if file_exists := path.isfile(file_path):
		mode = 'r+b' if write else 'rb'
	else:
		if write:
			mode = 'w+b'
		else:
			raise FileNotFoundError(ENOENT, strerror(ENOENT), file_path)

	if logger:
		logger.log(VERBOSE, f"Opening {file_path} with mode {mode}")

	f: BinaryIO = open(file_path, mode) # type: ignore[assignment]
	if file_exists:
		return read_header(f, logger), f

	return None, f


def get_inner_file_handle(
	fh: Union[str, BinaryIO], write: bool = False, logger: Optional[AmpelLogger] = None
) -> BinaryIO:
	""" override if needed """

	if isinstance(fh, str):
		_, fh = get_outer_file_handle(fh, write, logger) # type: ignore

	if write:
		mode = 'ab'
		fh.seek(0, 2)
	else:
		mode = 'rb'

	if fh.name.endswith('gz'):
		from gzip import GzipFile
		if logger: logger.log(VERBOSE, f"New GzipFile from {fh.name} (mode {mode})") # noqa: E701
		return GzipFile(fileobj=fh, mode=mode) # type: ignore[return-value]

	elif fh.name.endswith('bz2'):
		from bz2 import BZ2File
		if logger: logger.log(VERBOSE, f"New BZ2File from {fh.name} (mode {mode})") # noqa: E701
		return BZ2File(fh, mode=mode) # type: ignore[return-value]

	elif fh.name.endswith('xz'):
		from lzma import LZMAFile
		if logger: logger.log(VERBOSE, f"New LZMAFile from {fh.name} (mode {mode})") # noqa: E701
		return LZMAFile(fh, mode=mode) # type: ignore[return-value]

	return fh


def get_header_size(file_path: str) -> Optional[Tuple[int, int]]:
	with open(file_path, 'rb') as f:
		return _get_header_size(f.read(11))


def _get_header_size(b_header_info: bytes) -> Optional[Tuple[int, int]]:
	"""
	:returns: (header_size, header_len) if the header signature indicates
	an ampel register file (bytes([97, 109, 112, 101, 108])), None otherwise
	:raises: ValueError if file format is not recognized
	"""

	# First 11 bytes form the file signature:
	# bytes 0->5: b'ampel' (magic)
	# bytes 5->8: header block size (not including the first 11 bytes)
	# bytes 8->11: header length
	# Note: header is padded if block size != length
	if len(b_header_info) == 0: # empty file
		return None

	if len(b_header_info) != 11 or b_header_info[:5] != ampel_magic_bytes:
		raise ValueError(
			f"Unrecognized register file (first bytes: {b_header_info})" # type: ignore[str-bytes-safe]
		)
		return None

	# Block size of the header in the file
	header_size = int.from_bytes(b_header_info[5:8], 'little')

	# Length of the header within the block
	header_len = int.from_bytes(b_header_info[8:], 'little')

	return header_size, header_len


def get_header_content(file_path: str, verbose: bool = True) -> Optional[Dict[str, Any]]:
	""" :returns: header's content as dict """

	logger = AmpelLogger.get_logger() if verbose else None
	with open(file_path, "rb") as f:
		if d := read_header(f, logger):
			return d['payload']
	return None


def read_header(
	file_handle: BinaryIO, logger: Optional[AmpelLogger] = None
) -> Optional[HeaderInfo]:
	"""
	Reads and loads header using provided file handle
	:returns: a HeaderInfo typed dict if the provided file is a non-empty ampel register file
	(starts with the bytes([97, 109, 112, 101, 108])), None otherwise
	:raises: ValueError if file format is not recognized
	"""

	size_t = _get_header_size(file_handle.read(11)) # unsigned int
	if not size_t: # empty file
		return None

	header_size = size_t[0]
	header_len = size_t[1]

	h = file_handle.read(header_size)
	if len(h) == 0: # headerless file
		return None
	elif len(h) < header_size: # something is wrong
		raise ValueError(f"{file_handle.name}: header too small (len: {len(h)})")

	header = decode_header(h, header_len)

	if logger:
		logger.log(VERBOSE, f"Header size={header_size}, len={header_len}")
		logger.log(VERBOSE, f"================== {file_handle.name} header info ==================")
		logger.log(VERBOSE, json.dumps(header, indent=4))
		logger.log(VERBOSE, "=" * (50 + len(file_handle.name)))

	return HeaderInfo(size=header_size, len=header_len, payload=header)


def decode_header(b: bytes, length: int) -> Dict[str, Any]:
	""" Strip padding, possibly decompress and bson decode header """

	hdr = b[:length]

	# Zlib compressed payload starts with b'x\x9c'
	if hdr[0] == 120 and hdr[1] == 156:
		return bson.decode(decompress(bytes(hdr)))

	return bson.decode(bytes(hdr))


def open_file_and_write_header(file_path: str, header: Dict[str, Any], verbose: bool = False) -> None:

	logger = AmpelLogger.get_logger() if verbose else None
	with open(file_path, "r+b") as f:
		if hinfo := read_header(f, logger):
			write_header(f, header=header, hsize=hinfo['size'], logger=logger)
		elif verbose:
			logger.info(f"Unable to load header info from {file_path}") # type: ignore[union-attr]


def write_header(
	file_handle: BinaryIO, header: Union[Dict[str, Any], bytes], hsize: int,
	logger: Optional[AmpelLogger] = None, flush: bool = True
) -> None:
	""" Writes (potentialy padded and compressed) file header """

	if '+' not in file_handle.mode:
		raise ValueError(f"Cannot write header, incompatible file mode {file_handle.mode}")

	if isinstance(header, dict):
		header = bson.encode(header)

	hlen = len(header)

	if logger:
		logger.log(VERBOSE, f"Writing header (block_size={hsize}, len={hlen})")

	# try to compress header if we are above the max size
	# (takes up to 80 micro-seconds on a MBP for a 4000 bytes payload)
	if hlen > hsize:
		if logger:
			logger.warn(f"Header too long ({hlen} > {hsize}), trying to compress it")
		header = compress(header) # type: ignore[arg-type]
		hlen = len(header)
		if hlen > hsize:
			raise ValueError(f"Header too long ({hlen} > {hsize})")

	file_handle.seek(0, 0)

	# file's magic
	file_handle.write(b'ampel')

	# bytes 5->8 specify header block size
	file_handle.write(int.to_bytes(hsize, 3, 'little'))

	# bytes 8->11 specify header length within header block
	file_handle.write(int.to_bytes(hlen, 3, 'little'))

	# Write bson encoded header
	i = file_handle.write(header) # type: ignore[arg-type]

	if i != hsize:
		# Padding (offsets file cursor)
		file_handle.write(bytes([0] * (hsize - i)))

	if flush:
		file_handle.flush()


def rescale_header(
	file_path: str, new_size: int, remove_old_file: bool = False,
	header: Optional[Dict[str, Any]] = None
) -> None:

	from mmap import mmap, ACCESS_WRITE
	from os import rename, remove

	logger = AmpelLogger.get_logger()
	with open(file_path, "r+b") as f1:

		if not (hinfo := read_header(f1, None if header else logger)):
			return logger.info(f"Unable to load header info from {file_path}")

		if new_size < hinfo['len']:
			raise ValueError("New size is smaller that the header length")

		# stackoverflow.com/questions/32748231/preferred-block-size-when-reading-writing-big-binary-files
		logger.info(f"Creating new {file_path}.new")
		with open(f"{file_path}.new", "w+b") as f2:

			logger.info("Writing new header")
			write_header(f2, header=header if header else hinfo['payload'], hsize=new_size, logger=logger)

			with mmap(f1.fileno(), 0) as m1:

				hdr_len_diff = new_size - hinfo['size']
				logger.info(f"Header length difference: {hdr_len_diff}")

				# OS X does not support mmap resize
				f2.seek(hdr_len_diff + len(m1) - 1)
				f2.write(b'\0')
				f2.flush()

				logger.info("Writing register content")
				with mmap(f2.fileno(), len(m1) + hdr_len_diff, access=ACCESS_WRITE) as m2:
					m2[new_size:] = m1[hinfo['size']:] # performs copy

	if remove_old_file:
		logger.info(f"Removing {file_path}")
		remove(file_path)
	else:
		logger.info(f"Renaming {file_path} -> {file_path}.old")
		rename(file_path, f"{file_path}.old")

	logger.info(f"Renaming {file_path}.new -> {file_path}")
	rename(f"{file_path}.new", file_path)


def _quick_load(
	f: Union[BinaryIO, str], logger: Optional[AmpelLogger] = None
) -> Tuple[Optional[HeaderInfo], BinaryIO, Optional[BinaryIO]]:
	""" :raise: FileNotFoundError if provided file path (str) does not exist """

	if isinstance(f, str):
		hinfo, f = get_outer_file_handle(f, logger=logger)
	else:
		f.seek(0, 0)
		hinfo = read_header(f, logger)

	if hinfo is None:
		return None, f, None

	return hinfo, f, get_inner_file_handle(f, logger=logger)


def reg_iter(
	f: Union[BinaryIO, str], read_multiplier: int = 100000, verbose: bool = True
) -> Generator[Tuple[int, ...], None, None]:
	"""
	Iterates through unpacked blocks of a register
	:param f: file path (str) or file handle (which will not be closed)
	:param read_multiplier: (<struct size> * `read_multiplier`) bytes will be read at once iteratively from the provided file handle
	:returns: yields Tuple[<elements defined in struct>] ex: Tuple[<alert id>, <filter return code>]
	"""

	logger = AmpelLogger.get_logger() if verbose else None
	hinfo, ofh, ifh = _quick_load(f, AmpelLogger.get_logger() if verbose else None)
	if not hinfo or not ifh:
		if logger: logger.info("reg_iter() cannot continue as header info are missing") # noqa
		return


	struct = hinfo['payload']['struct']
	buf_len = calcsize(struct) * read_multiplier
	r = ifh.read

	while b := r(buf_len):
		for el in iter_unpack(struct, b):
			yield el

	ifh.close()
	if isinstance(f, str) and not ofh.closed:
		ofh.close()


def find(
	f: Union[BinaryIO, str],
	offset: int,
	match_int: Optional[Union[int, List[int]]] = None,
	int_bytes_len: Optional[int] = None,
	match_bytes: Optional[Union[bytes, List[bytes]]] = None,
	header_hint: Optional[str] = None,
	header_hint_callback: Optional[Callable] = None,
	read_multiplier: int = 100000, verbose: bool = True
) -> Optional[List[Tuple[int, ...]]]:
	"""
	:param f: file path (str) or file handle (which will be closed)
	:param read_multiplier: (<struct size> * `read_multiplier`) bytes will be read at once iteratively from the provided file handle
	:param match_bytes: bytes to match in the underlying stream (ex: to match an integer x < 255, use: int.to_bytes(x, 1, 'little'))
	:param offset: position of the field of interest within each block. (ex: if the block are made of '<QB' and you want
	to match the last byte, then offset should be set to 8)
	:returns: list of matching blocks

	Example:
	Let's consider MinimalRegister, it saves the alert id as a 8 bytes integer and the filter result as single byte.
	That forms blocks of 9 bytes each. The struct.pack argument is '<QB' -> (little endian) long long and one byte.
	If we wanted to search for any register entry with filter result "10", we would call:
	find(<file>, match_bytes=int.to_bytes(10, 1, 'little'), offset=8)
	(Since the filter result is the 9th byte of each block, we did set offset equals to 8).

	# Find alert by id using the general method find
	In []: find('/Users/hu/Documents/ZTF/test/aa/aa.bin.gz', match_bytes=int.to_bytes(1242886, 8, 'little'), offset=0)
	Out[]: [(1242886, 16)]

	# Find register entries with alert filter results equals 176 or 16
	In []: find('/Users/hu/Documents/ZTF/test/aa/aa.bin.gz', match_bytes=[
		int.to_bytes(176, 1, 'little'), int.to_bytes(16, 1, 'little')], offset=8)
	Out:[]: [(9659062, 16), (7559029, 176)]
	"""

	logger = AmpelLogger.get_logger() if verbose else None
	hinfo, ofh, ifh = _quick_load(f, AmpelLogger.get_logger() if verbose else None)
	if not hinfo or not ifh:
		if logger: logger.info("find() cannot continue as header info are missing") # noqa
		return None

	if match_int:

		if not int_bytes_len:
			# In the future, we might extract automcatically int_bytes_len from 'struct' (header)
			# using the value of parameter 'offset' and method struct.calcsize()
			raise ValueError("Parameter 'int_bytes_len' must be defined with 'match_int'")

		if header_hint and header_hint in hinfo['payload']:

			d = hinfo['payload'][header_hint]
			if isinstance(match_int, int):

				if not d['min'] <= match_int <= d['max']:
					if logger: logger.info(f"Fast header check: no match for {match_int}") # noqa: E701
					return None
			else:

				new_match_int = [
					el for el in match_int
					if (d['min'] <= match_int <= d['max'])
				]

				if not new_match_int:
					if logger: logger.info("Fast header check: no match for provided integers") # noqa: E701
					return None

				if len(new_match_int) != len(match_int):
					match_int = new_match_int
					if logger: logger.info(f"Fast header check: find reduced to: {match_int}") # noqa: E701
				elif logger:
					logger.info("Fast header check: target ids are all eligible")

		if header_hint_callback:
			match_int = header_hint_callback(hinfo['payload'], match_int, logger)
			if not match_int:
				if logger: logger.info("Fast header callback check: no match") # noqa: E701
				return None

	elif match_bytes:
		pass
	else:
		raise ValueError("Please provide either parameter 'match_bytes' or 'match_int' and 'int_bytes_len'")

	if match_int:
		match_bytes = convert_to_bytes(match_int, int_bytes_len) # type: ignore[arg-type]

	func = find_one if isinstance(match_bytes, bytes) else find_many
	struct = hinfo['payload']['struct']
	block_len = calcsize(struct)
	matches = func(ifh, block_len * read_multiplier, block_len, offset, match_bytes) # type: ignore[operator]

	ifh.close()
	if isinstance(f, str) and not ofh.closed:
		ofh.close()

	if matches:
		return list(iter_unpack(struct, b''.join(matches)))

	return None


def find_one(
	f: BinaryIO, read_len: int, block_len: int, offset: int, match_bytes: bytes
) -> List[bytes]:
	"""
	:param f: file handle
	:param read_len: number of bytes read at once iteratively from the provided file handle
	:param offset: position of the field of interest within each block. (ex: if register is made of '<QB' blocks
	and you want to match the last byte, then offset should be set to 8)
	:param match_bytes: bytes to match in the underlying stream
	:returns: list of matching bytes blocks
	"""

	ret = []
	while buf := f.read(read_len):
		start = 0
		while (i := buf.find(match_bytes, start)) != -1:
			start = i + 1
			pos = i - offset # block start position
			if pos % block_len == 0: # match concerns another field
				ret.append(buf[pos: pos + block_len])
	return ret


def find_many(
	f: BinaryIO, read_len: int, block_len: int, offset: int, match_bytes: List[bytes],
) -> List[bytes]:
	"""
	:param f: file handle
	:param read_len: number of bytes read at once iteratively from the provided file handle
	:param offset: position of the field of interest within each block. (ex: if register is made of '<QB' blocks
	and you want to match the last byte, then offset should be set to 8)
	:param match_bytes: list of bytes to match in the underlying stream
	:returns: list of matching bytes blocks
	"""

	ret = []
	while buf := f.read(read_len):
		for mb in match_bytes:
			start = 0
			while (i := buf.find(mb, start)) != -1:
				start = i + 1
				pos = i - offset # block start position
				if pos % block_len == 0: # match concerns another field
					ret.append(buf[pos: pos + block_len])
	return ret


def convert_to_bytes(arg: Union[int, List[int]], bytes_len: int) -> Union[bytes, List[bytes]]:
	if isinstance(arg, int):
		return int.to_bytes(arg, bytes_len, 'little')
	return [int.to_bytes(el, bytes_len, 'little') for el in arg]
