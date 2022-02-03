#!/usr/bin/env python3
import sys
import subprocess as sp
import multiprocessing
import concurrent.futures
import os
import glob

#Parallelises rm command to delete high inode count dirs
#usage:
#rm-turbo.py dir/ 24
#24 = threads


def paradel(i):
	print('deleting',i)
	
	p0=sp.Popen("rm -rf %s" %i,shell=True).wait()
	
def listdirs(rootdir):
	#dirs=[]
	#for it in os.scandir(rootdir):
		#if it.is_dir():
			#dirs.append(it.path)
			#listdirs(it,dirs)
	dirs=glob.glob(rootdir+"/*/*")
	for x in glob.glob(rootdir+"/*"):
		dirs.append(x)
	for x in glob.glob(rootdir):
		dirs.append(x)
	return dirs
	
def main():
	
	folder=sys.argv[1]
	threads=int(sys.argv[2])
	
	
	print("getting listings")
	g=listdirs(folder)
	#print(g)
	
	#g.reverse()
	print("deleting..")
	executor1 = concurrent.futures.ProcessPoolExecutor(threads)
	futures1 = [executor1.submit(paradel, i) for i in g]
	concurrent.futures.wait(futures1)

	p0=sp.Popen("rm -rf %s" %folder,shell=True).wait()
	#print("rm -rf %s" %folder)
	#print("rm-turbo completed")
	
if __name__ == '__main__': main()