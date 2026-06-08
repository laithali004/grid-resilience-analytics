This README.txt file was generated on 2023-05-26 by Sarah Tennille


GENERAL INFORMATION

1. Title of Dataset: EAGLE-I Power Outage Data 2014 - 2022

2. Author Information
	A. Principal Investigator Contact Information
		Name: Varisara Tansakul
		Institution: Oak Ridge National Laboratory
		Address: 1 Bethel Valley Road Oak Ridge, TN, 37830
		Email: tansakulv@ornl.gov

	B. Associate or Co-investigator Contact Information
		Name: Aaron Myers
		Institution: Oak Ridge National Laboratory
		Address: 1 Bethel Valley Road Oak Ridge, TN, 37830
		Email: myersat@ornl.gov

	C. Associate or Co-investigator Contact Information
		Name: Sarah Tennille
		Institution: Oak Ridge National Laboratory
		Address: 1 Bethel Valley Road Oak Ridge, TN, 37830
		Email: bleakneysa@ornl.gov

	D. Alternate Contact Information
		Name: EAGLE-I Webmaster
		Institution: Oak Ridge National Laboratory
		Address: 1 Bethel Valley Road Oak Ridge, TN, 37830
		Email: eagle-i@ornl.gov

3. Date of data collection (single date, range, approximate date): November 1 2014 - Dec 31 2022

4. Geographic location of data collection: United States & Territories


5. Information about funding sources that supported the collection of the data: Agreement number: 31256.
This material is based upon work supported by the U.S. Department of Energy’s Office of Cybersecurity, Energy, Security, and Emergency Response.


SHARING/ACCESS INFORMATION

1. Licenses/restrictions placed on the data: Creative Commons - Attribution 4.0 International (CC BY 4.0)(https://creativecommons.org/licenses/by/4.0/)

2. Links to publications that cite or use the data: 
https://kdd-milets.github.io/milets2021/papers/MiLeTS2021_paper_21.pdf
https://vtechworks.lib.vt.edu/handle/10919/105419
https://www.osti.gov/biblio/1923379 
https://vtechworks.lib.vt.edu/handle/10919/82747
https://dl.acm.org/doi/pdf/10.1145/3459637.3482410
https://ieeexplore.ieee.org/stamp/stamp.jsp?tp=&arnumber=10020733&tag=1
https://www.osti.gov/biblio/1894479
https://link.springer.com/chapter/10.1007/978-981-15-8983-6_18
https://trace.tennessee.edu/utk_gradthes/6438/
https://www.osti.gov/biblio/1887668/
https://ieeexplore.ieee.org/abstract/document/9984016
https://ieeexplore.ieee.org/abstract/document/10020811
https://ieeexplore.ieee.org/document/10012344


3. Links to other publicly accessible locations of the data: None

4. Links/relationships to ancillary data sets: N/A

5. Was data derived from another source?  No
	A. If yes, list source(s): N/A

6. Recommended citation for this dataset: 
Tansakul, Varisara, et al. (2023). "EAGLE-I Power Outage Data 2014 - 2022" [Data set]. DOI: (Coming soon)



DATA & FILE OVERVIEW

1. File List: 
eaglei_outages_2014.csv (78.1 MB)
eaglei_outages_2015.csv (599 MB)
eaglei_outages_2016.csv (619.8 MB)
eaglei_outages_2017.csv (698.8 MB)
eaglei_outages_2018.csv (999.2 MB)
eaglei_outages_2019.csv (1.1 GB)
eaglei_outages_2020.csv (1.17 GB)
eaglei_outages_2021.csv (1.14 GB)
eaglei_outages_2022.csv 1.03 GB)
coverage_history.csv (12 KB)


2. Relationship between files, if important: The coverage history file explains the percentage of electric utility customers captured in the EAGLE-I dataset for each year by state. This helps users understand what is included and missing in terms of customers in the all of the outage files.

3. Additional related data collected that was not included in the current data package: N/A

4. Are there multiple versions of the dataset? No
	A. If yes, name of file(s) that was updated: N/A
		i. Why was the file updated? N/A
		ii. When was the file updated? N/A


METHODOLOGICAL INFORMATION

1. Description of methods used for collection/generation of data: The Environment for the Analysis of Geo-Located Energy Information (EAGLE-I) is maintained at Oak Ridge National Laboratory (ORNL) for the Department of Energy (DOE). EAGLE-I is DOE’s operational and scalable data and information platform for real-time wide-area situational awareness of the energy sector, providing a centralized platform for monitoring power distribution outage for the US and Territories. This data has been collected from electric utility’s public outage maps using an ETL process.


2. Methods for processing the data: 
Data has been aggregated to the county level for the dissemination of the dataset.

3. Instrument- or software-specific information needed to interpret the data: N/A

4. Standards and calibration information, if appropriate: N/A

5. Environmental/experimental conditions: N/A

6. Describe any quality-assurance procedures performed on the data: Our coverage numbers are updated annually using the EIA-861 dataset. The EAGLE-I team daily monitors for parser errors to ensure the accuracy of our collected outage numbers, issuing releases twice a week or more as needed to maintain our system.

7. People involved with sample collection, processing, analysis and/or submission: Varisara Tansakul, Aaron Myers, Sarah Tennille, Matthew Denman, Alec Hamaker, Jonathan Huihui, Kyle Medlen, Karl Allen, Daniel Redmon, Supriya Chinthavali, Mark Colleti, Joshua Grant, Matt Lee, Dakotah Maguire, Scott Newby, Chelsey Stahl, Budhu Bhaduri, Jibo Sanyal


DATA-SPECIFIC INFORMATION FOR: 
eaglei_outages_2014.csv (78.1 MB)
eaglei_outages_2015.csv (599 MB)
eaglei_outages_2016.csv (619.8 MB)
eaglei_outages_2017.csv (698.8 MB)
eaglei_outages_2018.csv (999.2 MB)
eaglei_outages_2019.csv (1.1 GB)
eaglei_outages_2020.csv (1.17 GB)
eaglei_outages_2021.csv (1.14 GB)
eaglei_outages_2022.csv 1.03 GB)

1. Number of variables: 5

2. Number of cases/rows: 
eaglei_outages_2014.csv:  1,689,461
eaglei_outages_2015.csv: 12,939,158
eaglei_outages_2016.csv: 13,306,025
eaglei_outages_2017.csv: 15,078,365
eaglei_outages_2018.csv: 21,776,807
eaglei_outages_2019.csv: 24,074,123
eaglei_outages_2020.csv: 25,545,518
eaglei_outages_2021.csv: 24,826,103
eaglei_outages_2022.csv: 22,327,834

3. Variable List: 
 - Fips_code: The FIPS code of the county in which the power outages occurred, for example “12011”
 - County: The county name in which the power outages occurred spelled out in text, for example “Broward”
 - State: The state in which the power outage occurred, spelled out in full in text format. For example, “Florida”
 - Sum: The total number of customers without power for that county at that timestamp. This number is always an integer. Entries with 0 customers without power were not included in this dataset. Note that the number of “customers” does not necessarily equate to the number of people affected, as a “customer” reported by a utility could be one meter, one building, etc. Outages are collected at various different levels of resolution (county, point, zip code, and polygon) and are aggregated to county for consistent reporting.
 - Run_start_time: Date and timestamp provided in GMT in the format “MM/DD/YY 00:00”. EAGLE-I collects power outage information from all covered utilities at 15-minute intervals, and this timestamp marks the beginning of the collection run.


4. Missing data codes: N/A

5. Specialized formats or other abbreviations used: N/A


DATA-SPECIFIC INFORMATION FOR: coverage_history.csv

1. Number of variables: 7

2. Number of cases/rows: 281

3. Variable List: 
 - Year: The date used to derive the coverage for the given year. For example, the entry “1/1/2019” means all following information in the row is based on the date of Jan 1, 2019.  
 - State: Two character ID representing state, for example “AL” for Alabama
 - Total Customers: The total number of utility customers in the state on that given date. Customers are derived from the closest EIA-861 customer number updates to the provided date. 
 - Min_covered: The minimum number of utility customers covered by EAGLE-I in a given calendar year, as reported by EIA-861 
 - Max_covered: The maximum number of utility customers covered by EAGLE-I in a given calendar year, as reported by EIA-861 
 - Min_pct_covered: The minimum percentage of coverage for the state seen in a given year 
 - Max_pct_covered: The maximum percentage of coverage for the state seen in a given year 


4. Missing data codes: N/A

5. Specialized formats or other abbreviations used: N/A

RESOURCES
This material is based upon work supported by the U.S. Department of Energy’s Office of Cybersecurity, Energy, Security, and Emergency Response.
See https://eagle-i.doe.gov for more information.
