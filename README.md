This is an effort to download Florida election results in near-real time, and convert them into the Elex format. This was a joint effort between the Palm Beach Post and the Miami Herald. Those with questions can email Mike Stucka at mstucka@pbpost.com  and Caitlin Ostroff at costroff@mcclatchydc.com.

This is an effort to parse county-level results for Miami-Dade, Broward, Monroe and Manatee. It also pulls from the state's county-wide file.

## Setup: Install the requirements
`$ pip install -r requirements.txt `

## The Files:
* `bash missionControl.sh` runs the scrapers and the parsers to pull election results
  * `python Dade.py` Dade County scraper
  * `python Broward.py` Broward County scraper
  * `python Monroe.py` Monroe County scraper
  * `python Manatee.py` Manatee County scraper
  * `python composite_csvs.py` Merges all of the county results into one Elex-formatted csv and spits out `resultscomposite.json`. This currently commented out becasue the Herald wound up not using it.
  * `python calculations.py` Does all of the math for the races and spits out `stateagg.json` and `countyagg.json` for an aggregate count of the races

## The Data
Data is stored in the `/snapshots` directory followed by the county name for each
