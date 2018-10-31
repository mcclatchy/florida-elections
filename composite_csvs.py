from slugify import slugify # awesome-slugify, from requirements

import configuration    # configuration.py, with user-defined variables.

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

datadir = configuration.snapshotsdir
# resultscomposite = configuration.resultscomposite

def composite_csvs():
    global resultscomposite
    global datadir
    lineheaders = ['statename', 'description', 'uncontested', 'polid', 'electiondate',
                   'precinctsreportingpct', 'winner', 'precinctsreporting', 'incumbent',
                   'runoff', 'test', 'delegatecount', 'lastupdated', 'racetype', 'polnum',
                   'votecount', 'electtotal', 'first', 'is_ballot_measure', 'national',
                   'reportingunitname', 'racetypeid', 'level', 'votepct', 'id', 'last',
                   'ballotorder', 'officename', 'seatnum', 'seatname', 'precinctstotal',
                   'raceid', 'electwon', 'party', 'initialization_data', 'candidateid',
                   'reportingunitid', 'statepostal', 'fipscode', 'officeid']
    sourcecsvs = sorted(list(glob.glob(datadir + "*")))
    masterlist = []
    for filename in sourcecsvs:
        newestdirectories = sorted(list(glob.glob(filename +"/*")))
        sorted_files = sorted(newestdirectories, key=os.path.getctime)
        newestdirectory = sorted_files[-1]
        csvs = sorted(list(glob.glob(newestdirectory +"/*.csv")))
        for filename in csvs:
            if '/snapshots/Florida/' not in filename:
                with open(filename, "r") as csvfile:
                    reader = list(csv.DictReader(csvfile))
                # if list(reader[0].keys()) != lineheaders:
                #     print(list(reader[0].keys()))
                #     print("CSV input file " + filename + " has different headers than we're looking for. Not importing.")
                # else:
                #     print("CSV input file " + filename + " seems to fit Elex standard. Importing.")
                    for row in reader:
                        masterlist.append(row)
    with open('resultscomposite.csv', "w", newline="") as compositefile:
        writer = csv.DictWriter(compositefile, fieldnames=lineheaders)
        writer.writeheader()
        writer.writerows(masterlist)

    csvfile = open('resultscomposite.csv', 'r')
    jsonfile = open('resultscomposite.json', 'w')
    data = []
    fieldnames = lineheaders
    reader = csv.DictReader(csvfile, fieldnames)
    for row in reader:
        data.append(row)
    jsonfile.write(json.dumps(data, jsonfile))

composite_csvs()
