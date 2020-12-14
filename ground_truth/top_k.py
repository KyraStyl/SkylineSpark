#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 14 11:45:57 2020

@author: mavroudo
"""

import csv,sys
import matplotlib.pyplot as plt
def read_from_csv(filename):
    data=[]
    with open(filename,"r") as csvfile:
        for row in csv.reader(csvfile,delimiter=","):
            data.append(list(map(float,row)))
    return data

def a_dominates_b(a:list,b:list):
    for i,dimension in enumerate(a):
        if dimension>=b[i]:
            return False
    return True


class Point:
    def __init__(self,dims):
        self.dims=dims
        
    def __hash__(self):
        return sum([hash(i) for i in self.dims])
    
    def __repr__(self):
        return ",".join(list(map(str,self.dims)))

def find_top_k_brute_force(points,k):
    top_k = list()
    for i in range(len(points)):
        n=0
        for j in range(len(points)):
            if i == j:
                continue
            if a_dominates_b(points[i], points[j]):
                n+=1
        top_k.append([Point(points[i]),n])
        
    return sorted(top_k,key=lambda x: x[1], reverse=True)[:k]




if __name__ == "__main__":
    filename=sys.argv[1]
    k=int(sys.argv[2])
    #filename="Data_Generation/dataset1.csv"
    points=read_from_csv(filename)
    top_k=find_top_k_brute_force(points,k)  
    for point in top_k:
        print(point)