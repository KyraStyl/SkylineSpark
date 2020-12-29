#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 24 09:22:07 2020

@author: mavroudo
"""
import sys, getopt
from numpy import random
from scipy.stats import truncnorm
import numpy as np
import matplotlib.pyplot as plt

help_me="""
This is a script to generate data in a form of csv (comma seperated)
Obligatory parameters:
    -d, --dimensions  Number of dimensions of the data
    -p, --points      Number of elements to be created
    -o, --output      Name of the output file
    
Next define the type of distribution each dimension will follow. 
If a dimension is not described, then uniform will be used.

Optional parameters:
    -u,  --uniform    Followed by a list of ints that correspond to dimensions
    -a, --anticorrelated Followed by a list of ints. These dimensions will 
                    be in pairs
    -c, --correlated      Followed by a list of ints. These dimensions will 
                    be in pairs
    -n,, normal      Followed by a list of ints that correspond to dimensions
    
Example:
    
    datagen.py -d 2 -p 100 -o dataset1 -u 0,1 (do not leave space betten the dimensions)

"""




def get_options(argv):
    try:
        opts, args = getopt.getopt(argv,"hd:o:p:u:a:c:n:",["dimensions=","output=","points=","uniform=","anticorrelated=","correlated=","normal="])
    except:
        print(help_me)
        sys.exit(2)
    try:
        data={}
        for opt,arg in opts:
            if opt == '-h':
                print(help_me)
                sys.exit()
            elif opt in ("-d", "--dimensions"):
                data["dimensions"]=int(arg)
            elif opt in ("-o", "--output"):
                data["output"]=arg
            elif opt in ("-p", "--points"):
                data["points"]=int(arg)
            elif opt in ("-u", "--uniform"):
                data["uniform"]=list(map(int,arg.split(",")))
            elif opt in ("-a", "--anticorrelated"):
                data["anticorrelated"]=list(map(int,arg.split(",")))
                if len(data["anticorrelated"])%2!=0:
                    print("Dims in anticorrelated must be in pairs")
                    sys.exit(5)
            elif opt in ("-c", "--correlated"):
                data["correlated"]=list(map(int,arg.split(",")))
                if len(data["correlated"])%2!=0:
                    print("Dims in correlated must be in pairs")
                    sys.exit(5)
            elif opt in ("-n", "--normal"):
                data["normal"]=list(map(int,arg.split(",")))
        if "output" not in data or "dimensions" not in data or "points" not in data:
            print(help_me)
            sys.exit(3)
        return data
    except:
        print(help_me)
        sys.exit(4)



def generate_uniform(size:int):
    return min_max_normalization(list(random.uniform(low=0,high=1,size=size)))


def generate_normal(size:int):
    return min_max_normalization(list( random.normal(loc=1, scale=2, size=size)))


def generate_correlation(size:int,correlated=True):
    mean = [0.5,0.5]
    x=None
    if correlated:
        x=random.uniform(0.3,0.99) # pearson correlation
    else:
        x=random.uniform(-0.99,-0.3)
    print(x)
    r=[[1,float(x)],[float(x),1]]
    L = np.linalg.cholesky(r)
    uncorrelated = np.random.standard_normal((2, size))
    cor = np.dot(L, uncorrelated) + np.array(mean).reshape(2, 1)
    return min_max_normalization(list(cor[0,:])),min_max_normalization(list(cor[1,:]))

def min_max_normalization(dimension:list,minimum=0,maximum=1):
    newdata=[]
    minx=min(dimension)
    maxx=max(dimension)
    for d in dimension:    
        newdata.append((d-minx)/(maxx-minx)* (maximum-minimum) + minimum)
    return newdata
    
def combine_dimensions(data:dict,dimensions:int,points:int):
    for i in range(points):
        line=",".join([str(data[d][i]) for d in range(dimensions)])
        yield line+"\n"
        

if __name__ == "__main__":
    options=get_options(sys.argv[1:]) #read options and load to a dictinary
    print(options)
    size=options["points"]
    data={}
    for dim in range(options["dimensions"]):
        if "normal" in options and dim in options["normal"]: #generate normal dist
           data[dim]=generate_normal(size)
        elif "anticorrelated" in options and dim in options["anticorrelated"]: #generate anticorrelated
            pos=options["anticorrelated"].index(dim)
            if pos%2==0: #we calculate for this and the next one
                x1,x2=generate_correlation(size,False)
                data[dim]=x1
                data[options["anticorrelated"][pos+1]]=x2
        elif "correlated" in options and dim in options["correlated"]: #generate correlated
            pos=options["correlated"].index(dim)
            if pos%2==0: #we calculate for this and the next one
                x1,x2=generate_correlation(size,True)
                data[dim]=x1
                data[options["correlated"][pos+1]]=x2
        else: # the rest will be uniform
            data[dim]=generate_uniform(size)                
    #write to file
    with open(options["output"]+".csv","w") as f:
        for line in combine_dimensions(data,options["dimensions"],size):
            f.write(line)
    #print data if there are 2 d
    if options["dimensions"]==2:
        plt.scatter(data[0], data[1], c='green')
        plt.savefig(options["output"]+".png")
    
    
    
    
    
    
    
    
    
    
    