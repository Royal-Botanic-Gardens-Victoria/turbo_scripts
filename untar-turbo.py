#!/usr/bin/env python3
import sys
import subprocess as sp
import multiprocessing
import concurrent.futures
import re
import glob

#usage:
#untar-turbo.py "files/*.tar.gz" untar/ "*/*_files.fasta" 24

def tokenize(filename):
	digits = re.compile(r'(\d+)')
	return tuple(int(token) if match else token for token, match in ((fragment, digits.search(fragment)) for fragment in digits.split(filename)))


def parauntar(i):
	
	print('untaring',i)
	
	p0=sp.Popen("tar -zxf %s -C %s %s" %(i,outfolder,files),shell=True).wait()


folder=sys.argv[1] 
filelist=glob.glob(folder)
filelist.sort(key=tokenize)	

outfolder = sys.argv[2] #outfolder
p1=sp.Popen("mkdir -p %s" %outfolder,shell=True).wait()

files=sys.argv[3] #pattern of file name to extract

print(filelist)
threads=int(sys.argv[4])
	

	
if __name__ == '__main__':

	print("untaring..",threads,"threads")
	executor1 = concurrent.futures.ProcessPoolExecutor(threads)
	futures1 = [executor1.submit(parauntar,i) for i in filelist]
	concurrent.futures.wait(futures1)
	
	print("untar-turbo completed")