#-------------------------------------------------------------------------------
# Name:        WPDx Decision Support Toolset
# Purpose:     Tools for working with the Water Point Data Exchange
# Author:      Daniel Siegel
# Created:     14/01/2018
#-------------------------------------------------------------------------------

# make sure to install these packages before running:
# pip install sodapy

#useful doc is here:
#https://dev.socrata.com/foundry/data.waterpointdata.org/gihr-buz6
#https://github.com/xmunoz/sodapy#getdataset_identifier-content_typejson-kwargs


from os.path import join
from os.path import dirname

myScripts = join(dirname(__file__), "Scripts")
sys.path.append(myScripts)

from Repair import RepairPriority
from Overview import ServiceOverview

class Toolbox(object):
    def __init__(self):
        """Tools for working with the Water Point Data Exchange"""
        self.label = "WPDx Decision Support Toolset"
        self.alias = ""
        self.tools = [RepairPriority, ServiceOverview]


