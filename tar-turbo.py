#!/usr/bin/env python3
import sys
import subprocess as sp
import multiprocessing
import concurrent.futures
import os

#Parallelises taring dirs
#usage:
#tar-turbo.py inputdir/ outdir/ 24

def paratar(folder,i,outfolder):
	
	name1=i.split("/")[-1].split("_")[0]
	
	print("cd %s;tar -czf %s/%s.tar.gz %s/" %(folder,outfolder,name1,i))
	
	p0=sp.Popen("cd %s;tar -czf %s/%s.tar.gz %s/" %(folder,outfolder,name1,i),shell=True).wait()


	
def main():
	
	folder=sys.argv[1] #/g/data/nm31/d/r3.7_hybpiper_run_gap/04_processed_gene_directories/
	outfolder=sys.argv[2] #/g/data/nm31/gap_results/phylogenomics/04_processed_gene_directories_tar/
	threads=int(sys.argv[3])
	
	dirs=[]
	print("getting listings")
	h=os.listdir(folder)
	#print(h)
	
	#g.reverse()
	print("taring..")
	executor1 = concurrent.futures.ProcessPoolExecutor(threads)
	futures1 = [executor1.submit(paratar, folder, i,outfolder) for i in h]
	concurrent.futures.wait(futures1)

	
	
	print("tar-turbo completed")
	
if __name__ == '__main__': main()