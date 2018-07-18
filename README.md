# WPDx Decision Support Toolset

Tools for working with the [Water Point Data Exchange](https://www.waterpointdata.org/) (WPDx), a global platform for sharing data on water distribution points in the developing world

 Through bringing together diverse data sets, the water sector can establish an unprecedented understanding of water services. Sharing this data has the potential to improve water access for millions of people as a result of better information available to governments, service providers, researchers, NGOs, and others. The WPDx already makes some [advanced analytics](https://data.waterpointdata.org/view/cn6c-zc2q) available on their website. This toolbox is intended to extend these capabilities and support decision makers by geo-enriching the water points with data about population disbtribution.
 
 ![WPDx Toolset in ArcCatalog](/Data/Screenshots/Screenshot.jpg)
 
These tools require a license for the Spatial Analyst extention of ArcGIS, and the following Python packages:
* arcpy
* sodapy 

They can be run in ArcMap or from the command line. Before you run the tools in any specific country, make sure to run through all five steps in the 'New_Country_Checklist.txt' file.

 
## New Locations

Finds optimal locations for new water points that maximize population served.	

 ![New Locations](/Data/Screenshots/Screenshot1.jpg)		   
						   
## Repair Priority
   
Prioritizes water points for repair by estimating how many people are affected by each broken water point.

 ![Repair Priority](/Data/Screenshots/Screenshot2.jpg)

## Service Overview

Assesses access to safe water in each administrative area based on known water points and population distribution data.

 ![Service Overview](/Data/Screenshots/Screenshot3.jpg)


