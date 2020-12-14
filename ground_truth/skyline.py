#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 14 10:58:28 2020

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

def find_skyline_brute_force(points):
    skyline = set()
    for i in range(len(points)):
        dominated = False
        for j in range(len(points)):
            if i == j:
                continue
            if a_dominates_b(points[j], points[i]):
                dominated = True
                break
        if not dominated:
            skyline.add(i)

    return [points[i] for i in skyline]




if __name__ == "__main__":
    filename=sys.argv[1]
    #filename="Data_Generation/dataset1.csv"
    points=read_from_csv(filename)
    sk=find_skyline_brute_force(points) 
    plt.scatter([i[0] for i in points],[i[1] for i in points],c='C0')
    plt.scatter([i[0] for i in sk],[i[1] for i in sk],c='C1')
    plt.savefig("answer.png")
    for point in sk:
        print(point)