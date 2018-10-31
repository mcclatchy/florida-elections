from decimal import *
from slugify import slugify # awesome-slugify, from requirements

import configuration    # configuration.py, with user-defined variables.
from pandas import DataFrame, read_csv
from pandas import datetime as dt
import pandas as pd #this is how I usually import pandas
import sys #only needed to determine Python version number
import matplotlib #only needed to determine Matplotlib version number
import numpy as np
import csv
import glob
import time
import datetime
from collections import OrderedDict
import pprint
import os
import sys
from subprocess import Popen
import pickle
import json
import pandas

def FloridaResults():
    targetdir = configuration.snapshotsdir

    sourcecsvs = sorted(list(glob.glob(targetdir + "*")))
    for filename in sourcecsvs:
        newestdirectories = sorted(list(glob.glob(filename +"/*")))
        sorted_files = sorted(newestdirectories, key=os.path.getctime)
        newestdirectory = sorted_files[-1]
        csvs = sorted(list(glob.glob(newestdirectory +"/*.csv")))
        for filename in csvs:
            if '/snapshots/Florida/' in filename:
                df = pd.read_csv(filename, dtype='object', sep=',', encoding="utf8")
                df['precinctsreporting'] = df['precinctsreporting'].astype('int64')
                df['precinctstotal'] = df['precinctstotal'].astype('int64')
                df['votecount'] = df['votecount'].astype('int64')
                votestotal = df.groupby(['officename', 'last', 'first'])['precinctsreporting', 'precinctstotal', 'votecount'].sum()
                votestotal['totalvotesinrace'] = votestotal.groupby(['officename'])['votecount'].transform('sum')
                votestotal['votespct'] = votestotal['votecount']/votestotal['totalvotesinrace']
                votestotal['precinctsreportingpct'] = votestotal['precinctsreporting']/votestotal['precinctstotal']
                votestotal.to_csv("stateagg.csv")
                csvfile = open('stateagg.csv', 'r')
                jsonfile = open('stateagg.json', 'w')
                data = []
                reader = csv.DictReader(csvfile)
                for row in reader:
                    data.append(row)
                jsonfile.write(json.dumps(data, jsonfile))

FloridaResults()

def countyResults():
    targetdir = configuration.snapshotsdir
    files = []
    dfs = []
    sourcecsvs = sorted(list(glob.glob(targetdir + "*")))
    for filename in sourcecsvs:
        newestdirectories = sorted(list(glob.glob(filename +"/*")))
        sorted_files = sorted(newestdirectories, key=os.path.getctime)
        newestdirectory = sorted_files[-1]
        csvs = sorted(list(glob.glob(newestdirectory +"/*.csv")))
        for filename in csvs:
            if '/snapshots/Miami-Dade/' in filename:
                df = pd.read_csv(filename, dtype='object', sep=',', encoding="utf8")
                df['id'] = df['id'].str.title()
                df['raceid'] = df['raceid'].str.title()
                df['racetype'] = df['racetype'].str.title()
                df['officename'] = df['officename'].str.title()
                df['party'] = df['party'].str.title()
                df['party'] = df['party'].astype(str)
                df['party'] = df['party'].replace(['Rep', 'Dem', 'Npa', 'Lpf', 'Ref'],['Republican Party', 'Democratic Party', 'No Party Affiliation', 'Libertarian Party', 'Reform Party'])
                dfs.append(df)
            if '/snapshots/Monroe' in filename or '/snapshots/Broward' in filename or '/snapshots/Manatee' in filename:
                files.append(filename)
                for filename in files:
                    df = pd.read_csv(filename, dtype='object', sep=',', encoding="utf8")
                    df = pd.DataFrame(df)
                    df['precinctsreporting'] = df['precinctsreporting'].astype('float')
                    df['precinctstotal'] = df['precinctstotal'].astype('float')
                    df['votecount'] = df['votecount'].astype('float')
                    df['votepct'] = df['votepct'].astype('float')
                    df['totalvotesinrace'] = df.groupby(['officename'])['votecount'].transform('sum')
                    df['votepct'] = df['votecount']/df['totalvotesinrace']
                    df['precinctsreportingpct'] = df['precinctsreporting']/df['precinctstotal']
                    dfs.append(df)
            r = pd.concat([df for df in dfs], axis=0, sort=True)
            df_clean = r.drop_duplicates()
            df_clean.to_csv('countyagg.csv')
            csvfile = open('countyagg.csv', 'r')
            jsonfile = open('countyagg.json', 'w')
            data = []
            reader = csv.DictReader(csvfile)
            for row in reader:
                data.append(row)
            jsonfile.write(json.dumps(data, jsonfile))


countyResults()
