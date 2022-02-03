#!/usr/bin/env python3
import sys
import re
import glob
import subprocess

#counts reads in files in a folder in parallel
#usage:
#turbo-count.py "files/*" counts.txt 24
#24 = threads

digits = re.compile(r'(\d+)')
def tokenize(filename):
    return tuple(int(token) if match else token
                 for token, match in
                 ((fragment, digits.search(fragment))
                  for fragment in digits.split(filename)))



folder = sys.argv[1] 

filelist=glob.glob(folder)

filelist.sort(key=tokenize)
print(filelist)

g=sys.argv[2]

threads=int(sys.argv[3])

c=-1
p1=[]

type1=filelist[0].split("/")[-1][-4:]

if type1=="q.gz":
	while c<len(filelist)-1:
		
		while len(p1)<threads:
			if c<len(filelist)-1:
				c=c+1
				file1=filelist[c]
				print(file1)
				p1.append(subprocess.Popen("zcat %s | echo %s $((`wc -l`/4)) >> %s" %(file1,file1,g),shell=True))
			else:
				break
		n=-1
		for x in p1:
			n=n+1
			if x.poll()!=None:
				del p1[n]
				
		
				
if type1=="astq" or type1[-3]==".fq":
	while c<len(filelist)-1:
		
		while len(p1)<threads:
			if c<len(filelist)-1:
				c=c+1
				file1=filelist[c]
				print(file1)
				p1.append(subprocess.Popen("cat %s | echo %s $((`wc -l`/4)) >> %s"  %(file1,file1,g),shell=True))
			else:
				break
		n=-1
		for x in p1:
			n=n+1
			if x.poll()!=None:
				del p1[n]
				
if type1=="asta" or or type1==".fna" or type1[-3]==".fa":
	while c<len(filelist)-1:
		
		while len(p1)<threads:
			if c<len(filelist)-1:
				c=c+1
				file1=filelist[c]
				print(file1)
			
				p1.append(subprocess.Popen("grep -Hc '>' %s >> %s"  %(file1,g),shell=True))
			else:
				break
		n=-1
		for x in p1:
			n=n+1
			if x.poll()!=None:
				del p1[n]
				
print("WARNING: check top until wc finishes")
			
				
			