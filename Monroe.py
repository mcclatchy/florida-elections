#Manatee does not report provisional ballots
import urllib.request
from bs4 import BeautifulSoup
import re
import time
import csv
import html5lib
import os
import os.path
import datetime
from slugify import slugify
import configuration   # Local file, configuration.py, with settings
import clarityparser    # Local file, clarityparser.py


countyname = "Monroe"    # Critical to have this!
rawtime = datetime.datetime.now()
snapshotsdir = configuration.snapshotsdir
timestamp = datetime.datetime.strftime(rawtime, "%Y%m%d-%H%M%S")
filepath = snapshotsdir + (countyname) + "/" + timestamp + "/"
raceResults = []
lastupdated = datetime.datetime.strftime(rawtime, "%Y-%m-%dT%H:%M:%S")
electiondate = '11/6/2018'
level = 'subunit'
def getResults():

    html = urllib.request.urlopen("https://enr.electionsfl.org/MON/Summary/1997/")
    bsObj = BeautifulSoup(html, "html5lib")
    #find all of the races
    resultsSection = bsObj.findAll("div", {"class":"Race row"})
    #for each of the races
    for result in resultsSection:
        #get the race name, i.e. senator
        raceName = result.find("div", {"class":"row RaceName"})
        precinctsReporting = result.find("span", {"class":"numPrecinctsReported"})
        precinctsParticipating = result.find("span", {"class":"numPrecinctsParticipating"})
        section = result.find("tbody")
        trs = section.findAll("tr")
        for tr in trs:
            name = tr.find("td", {"class":"ChoiceColumn"})
            name = (name.get_text())
            if '(REP)' in name:
                name = name.replace('(REP)', "")
                party = 'Republican Party'
            elif '(DEM)' in name:
                name = name.replace('(DEM)', "")
                party = 'Democratic Party'
            elif '(STATS)' in name:
                name = name.replace('(STATS)', "")
                party = 'Nonpartisan'
            name = name.strip().split()
            first = name[0]
            last = name[-1]
            if 'Jr' in last or 'Sr' in last:
                last = name[-2]
            party = party.strip()
            electionDayVotes = tr.find("td", {"class":"DetailResultsColumn notranslate PollingVotes"})
            voteByMail = tr.find("td", {"class":"DetailResultsColumn notranslate MailVotes"})
            earlyVotes = tr.find("td", {"class":"DetailResultsColumn notranslate EarlyVotes"})
            provisionalVotes = tr.find("td", {"class":"DetailResultsColumn notranslate ProvisionalVotes"})
            totalVotes = tr.find("td", {"class":"DetailResultsColumn notranslate TotalVotes"})
            percentOfVotes = tr.find("td", {"class":"DetailResultsColumn notranslate"})
            raceResult = {
                'officename': raceName.get_text().strip(),
                'first': first,
                'last': last,
                'party': party,
                'precinctsreporting': precinctsReporting.get_text().strip(),
                'precinctstotal': precinctsParticipating.get_text().strip(),
                # 'electionDayVotes': electionDayVotes.get_text().replace(",", "").strip(),
                # 'voteByMail': voteByMail.get_text().replace(",", "").strip(),
                # 'earlyVotes': earlyVotes.get_text().replace(",", "").strip(),
                'votecount': totalVotes.get_text().replace(",", "").strip(),
                'votepct': percentOfVotes.get_text().replace("%", "").strip(),
                'reportingunitname': "Monroe",
                'statename': 'Florida',
                'statepostal': 'FL',
                'lastupdated': lastupdated,
                'electiondate': electiondate,
                'precinctsreportingpct': "",
                'level': level
            }
            raceResults.append(raceResult)
getResults()

def saveToCSV(raceResults):
    global driver
    os.makedirs(filepath, exist_ok=True)
    filename = countyname+ ".csv"
    #open your new csv file with a 'w' so you can write to it
    with open(filepath+filename, 'w') as output_file:
        #make headers for you columns. these must match up with the keys you set in your python dictionary, inamte
        fieldnames = ["id", "raceid", "racetype", "racetypeid", "ballotorder", "candidateid", "description",
                       "delegatecount", "electiondate", "electtotal", "electwon", "fipscode", "first", "incumbent",
                       "initialization_data", "is_ballot_measure", "last", "lastupdated", "level", "national",
                       "officeid", "officename", "party", "polid", "polnum", "precinctsreporting", "precinctsreportingpct",
                       "precinctstotal", "reportingunitid", "reportingunitname", "runoff", "seatname",
                       "seatnum", "statename", "statepostal", "test", "uncontested", "votecount", "votepct", "winner"
                       ]
        #write these into a csv, the headers being fieldnames and the rows your list of inmates
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(raceResults)

saveToCSV(raceResults)
