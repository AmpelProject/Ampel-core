import sys, os

def split_tar(tar_path, chunk_size=51000000):
	"""
	size: in bytes
	To run it with 20 workers in parallel, use: 
	ls ../tarballs/* | xargs -P 20 -n1 python3 split_tar.py
	"""

	fname = tar_path.split("/")[-1]

	if os.path.getsize(tar_path) < chunk_size:
		from shutil import copyfile
		print("%s smaller than chunk size, copying it" % tar_path)
		copyfile(tar_path, fname)
		return

	import tarfile
	in_tar = tarfile.open(tar_path, mode='r|gz')
	count = 0
	current_index = 1
	out_fname = "%s.part.%i" % (fname, current_index)
	print("Creating %s" % out_fname)
	out_tar = tarfile.open(out_fname, mode='w|gz')


	for ti in in_tar:

		out_tar.addfile(ti, in_tar.extractfile(ti))
		count += 1

		if count == 500:
			count = 0
			if os.path.getsize(out_fname) > chunk_size:
				total_len = 0
				out_tar.close()
				current_index += 1
				out_fname = "%s.part.%i" % (fname, current_index)
				print("Creating %s" % out_fname)
				out_tar = tarfile.open(out_fname, mode='w|gz')

	
	in_tar.close()
	out_tar.close()


def main():
	split_tar(sys.argv[1])

if __name__ == "__main__":
    main()
