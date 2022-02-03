#!/usr/bin/env python3
import sys
import subprocess as sp
import multiprocessing
import concurrent.futures
import re
import glob

#compresses files in a folder in parallel
#gzip-turbo.py files.list gzipfiles/ 24

def tokenize(filename):
	digits = re.compile(r'(\d+)')
	return tuple(int(token) if match else token for token, match in ((fragment, digits.search(fragment)) for fragment in digits.split(filename)))


def gzturbo(i):
	
	print('gzipping',i)
	name1=i.split("/")[-1]
	
	p0=sp.Popen("gzip -c  %s >%s/%s" %(i,outfolder,name1+".gz"),shell=True).wait()


f=open(sys.argv[1],'r')

filelist=[]
for x in f:
	filelist.append(x.rstrip("\n"))

outfolder = sys.argv[2] #outfolder
p1=sp.Popen("mkdir -p %s" %outfolder,shell=True).wait()

threads=int(sys.argv[3])
	

	
if __name__ == '__main__':

	print("gzipping..",threads,"threads")
	executor1 = concurrent.futures.ProcessPoolExecutor(threads)
	futures1 = [executor1.submit(gzturbo,i) for i in filelist]
	concurrent.futures.wait(futures1)
	
	print("gzip-turbo completed")