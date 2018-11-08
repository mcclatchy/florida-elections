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

time = datetime.datetime.now().strftime("%Y-%m-%d %I:%M %p")

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
                df['seatname'] = df['seatname'].fillna(" ")
                df['officename'] = df['officename'] + " " + df['seatname']
                df['precinctsreporting'] = df['precinctsreporting'].astype('int64')
                df['precinctstotal'] = df['precinctstotal'].astype('int64')
                df['votecount'] = df['votecount'].astype('int64')
                df['party'] = df['party'].fillna(" ")
                df['first'] = df['first'].fillna(" ")
                df['party'] = df['party'].replace(['Republican Party', 'Democratic Party', 'No Party Affiliation', 'Libertarian Party', 'Green Party', 'Reform Party', 'Non-Partisan'],['REP', 'DEM', 'NPA', 'LPF', 'GRE', 'REF', ''])
                df['last'] = df['last'].replace(['Fried',],['"Nikki" Fried'])
                df['lastupdated'] = time
                votestotal = df.groupby(['officename', 'last', 'first', 'party', 'lastupdated'])['precinctsreporting', 'precinctstotal', 'votecount'].sum()
                votestotal['totalvotesinrace'] = votestotal.groupby(['officename'])['votecount'].transform('sum')
                votestotal['totalvotesinrace'] = votestotal['totalvotesinrace'].astype('int64')
                votestotal['votepct'] = votestotal['votecount']/votestotal['totalvotesinrace']
                votestotal['precinctsreportingpct'] = votestotal['precinctsreporting']/votestotal['precinctstotal']
                # print(votestotal['party'])
                votestotal.to_csv("stateagg.csv")
                csvfile = open('stateagg.csv', 'r')
                jsonfile = open('stateagg.json', 'w')
                data = []
                reader = csv.DictReader(csvfile)
                for row in reader:
                    data.append(row)
                jsonfile.write(json.dumps(data, jsonfile))
                df = df.loc[(df['officename'] == 'Governor  ') | (df['officename'] == 'United States Senator  ')]
                countyturnout = df.groupby(['officename', 'last', 'first', 'party', 'reportingunitname'])['precinctsreporting', 'precinctstotal', 'votecount'].sum()
                countyturnout['totalvotesinrace'] = countyturnout.groupby(['officename'])['votecount'].transform('sum')
                countyturnout['totalvotesinrace'] = countyturnout['totalvotesinrace'].astype('int64')
                countyturnout['votepct'] = countyturnout['votecount']/countyturnout['totalvotesinrace']
                countyturnout['precinctsreportingpct'] = countyturnout['precinctsreporting']/countyturnout['precinctstotal']
                countyturnout.to_csv("countyturnout.csv")
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
                df['party'] = df['party'].replace(['Rep', 'Dem', 'Npa', 'Lpf', 'Ref', 'nan'],['REP', 'DEM', 'NPA', 'LPF', 'REF', ''])
                df['first'] = df['first'].replace(['YE', 'N'],['Yes', 'No'])
                df['last'] = df['last'].replace(['S', 'O'],['', ''])
                df.loc[df.officename.str.contains('Const Amend'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Attorney Gen'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Chief Financial Officer'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Circuit Judge'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Commissioner Of Agriculture'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Governor/'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Representative In Congress'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('State Repres'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('State Sen'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Justice Of Supreme Court'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Court Of Appeal'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('United States Senator'), 'national'] = 'Yes'
                df['lastupdated'] = time
                df = df.drop(df[df['national']=='Yes'].index)
                df = df.drop_duplicates()
                df.to_csv('miamiagg.csv', index=False)
                csvfile = open('miamiagg.csv', 'r')
                jsonfile = open('miamiagg.json', 'w')
                data = []
                reader = csv.DictReader(csvfile)
                for row in reader:
                    data.append(row)
                jsonfile.write(json.dumps(data, jsonfile))
            elif '/snapshots/Monroe' in filename:
                df = pd.read_csv(filename, dtype='object', sep=',', encoding="utf8")
                df = pd.DataFrame(df)
                df['precinctsreporting'] = df['precinctsreporting'].astype('float')
                df['precinctstotal'] = df['precinctstotal'].astype('float')
                df['votecount'] = df['votecount'].astype('float')
                df['votepct'] = df['votepct'].astype('float')
                df['totalvotesinrace'] = df.groupby(['officename'])['votecount'].transform('sum')
                df['votepct'] = df['votecount']/df['totalvotesinrace']
                df['precinctsreportingpct'] = df['precinctsreporting']/df['precinctstotal']
                df['party'] = df['party'].replace(['Republican Party', 'Democratic Party', 'No Party Affiliation', 'Nonpartisan'],['REP', 'DEM', 'NPA', ''])
                df.loc[df.officename.str.contains('Const Amnd'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Attorney Gen'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Chief Financial Officer'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Circuit Judge'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Commissioner of Agriculture'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Governor'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('State Repres'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('State Sen'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Justice of the Supreme Court'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Court of Appeal'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('United States'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Const Rev'), 'national'] = 'Yes'
                df['lastupdated'] = time
                df = df.drop(df[df['national']=='Yes'].index)
                df.to_csv('monroeagg.csv', index=False)
                csvfile = open('monroeagg.csv', 'r')
                jsonfile = open('monroeagg.json', 'w')
                data = []
                reader = csv.DictReader(csvfile)
                for row in reader:
                    data.append(row)
                jsonfile.write(json.dumps(data, jsonfile))
            elif '/snapshots/Broward' in filename:
                df = pd.read_csv(filename, dtype='object', sep=',', encoding="utf8")
                df = pd.DataFrame(df)
                df['precinctsreporting'] = df['precinctsreporting'].astype('float')
                df['precinctstotal'] = df['precinctstotal'].astype('float')
                df['votecount'] = df['votecount'].astype('float')
                df['votepct'] = df['votepct'].astype('float')
                df['totalvotesinrace'] = df.groupby(['officename'])['votecount'].transform('sum')
                df['votepct'] = df['votecount']/df['totalvotesinrace']
                df['precinctsreportingpct'] = df['precinctsreporting']/df['precinctstotal']
                df['party'] = df['party'].replace(['Republican Party', 'Democratic Party', 'No Party Affiliation', 'Nonpartisan'],['REP', 'DEM', 'NPA', ''])
                df.loc[df.officename.str.contains('Constitutional'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Attorney Gen'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Chief Financial Officer'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Circuit Judge'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Commissioner of Agriculture'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Governor'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Representative in Congress'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('State Repres'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('State Sen'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Supreme Court'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('Court of Appeal'), 'national'] = 'Yes'
                df.loc[df.officename.str.contains('United States Senator'), 'national'] = 'Yes'
                df['last'] = df['last'].replace(['Yes', 'No', 'For', 'Against', 'WRITE-IN'],['', '', '', '', ''])
                df['lastupdated'] = time
                df = df.drop(df[df['national']=='Yes'].index)
                df.to_csv('browardagg.csv', index=False)
                csvfile = open('browardagg.csv', 'r')
                jsonfile = open('browardagg.json', 'w')
                data = []
                reader = csv.DictReader(csvfile)
                for row in reader:
                    data.append(row)
                jsonfile.write(json.dumps(data, jsonfile))
            elif '/snapshots/Manatee' in filename:
                 df = pd.read_csv(filename, dtype='object', sep=',', encoding="utf8")
                 df = pd.DataFrame(df)
                 df['precinctsreporting'] = df['precinctsreporting'].astype('float')
                 df['precinctstotal'] = df['precinctstotal'].astype('float')
                 df['votecount'] = df['votecount'].astype('float')
                 df['votepct'] = df['votepct'].astype('float')
                 df['totalvotesinrace'] = df.groupby(['officename'])['votecount'].transform('sum')
                 df['votepct'] = df['votecount']/df['totalvotesinrace']
                 df['precinctsreportingpct'] = df['precinctsreporting']/df['precinctstotal']
                 df.loc[df.officename.str.contains('Representative in Congress'), 'national'] = 'Yes'
                 df.loc[df.officename.str.contains('Attorney Gen'), 'national'] = 'Yes'
                 df.loc[df.officename.str.contains('Chief Financial Officer'), 'national'] = 'Yes'
                 df.loc[df.officename.str.contains('Circuit Judge'), 'national'] = 'Yes'
                 df.loc[df.officename.str.contains('Commissioner of Agriculture'), 'national'] = 'Yes'
                 df.loc[df.officename.str.contains('Governor'), 'national'] = 'Yes'
                 df.loc[df.officename.str.contains('State Repres'), 'national'] = 'Yes'
                 df.loc[df.officename.str.contains('State Sen'), 'national'] = 'Yes'
                 df.loc[df.officename.str.contains('Darryl C Casanueva'), 'national'] = 'Yes'
                 df.loc[df.officename.str.contains('Anthony K Black'), 'national'] = 'Yes'
                 df.loc[df.officename.str.contains('Alan Lawson'), 'national'] = 'Yes'
                 df.loc[df.officename.str.contains('Edward C LaRose'), 'national'] = 'Yes'
                 df.loc[df.officename.str.contains('Susan H Rothstein-Youakim'), 'national'] = 'Yes'
                 df.loc[df.officename.str.contains('Court of Appeal'), 'national'] = 'Yes'
                 df.loc[df.officename.str.contains('United States'), 'national'] = 'Yes'
                 df.loc[df.officename.str.contains('City of Holmes Beach Mayor Mayor'), 'officename'] = 'City of Holmes Beach Mayor'
                 df.loc[df.officename.str.contains('City of Holmes Beach Commission Commission'), 'officename'] = 'City of Holmes Beach Commission'
                 df.loc[df.officename.str.contains('Amendment'), 'national'] = 'Yes'
                 df.loc[df.officename.str.contains('School Board Referendum'), 'last'] = ''
                 df['party'] = df['party'].replace(['Republican Party', 'Democratic Party', 'Nonpartisan'],['REP', 'DEM', ''])
                 df['lastupdated'] = time
                 df = df.drop(df[df['national']=='Yes'].index)
                 df.to_csv('manateeagg.csv', index=False)
                 csvfile = open('manateeagg.csv', 'r')
                 jsonfile = open('manateeagg.json', 'w')
                 data = []
                 reader = csv.DictReader(csvfile)
                 for row in reader:
                     data.append(row)
                 jsonfile.write(json.dumps(data, jsonfile))
countyResults()
