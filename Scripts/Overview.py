# core libraries
import arcpy
from os.path import join
from os.path import dirname
from sodapy import Socrata
import csv
import tempfile
import time

class ServiceOverview(object):
    def __init__(self):
        """Estimates access to safe water by administrative area."""
        self.label = 'Service Overview'
        self.description = 'Estimates access to safe water by ' + \
                           'administrative area.'
        self.canRunInBackground = True

    def getParameterInfo(self):
        """Define parameter definitions"""
        Param0 = arcpy.Parameter(
                        displayName='Country',
                        name='zone',
                        datatype='GPString',
                        parameterType='Required',
                        direction='Input')

        Param1 = arcpy.Parameter(
                        displayName='Access Distance',
                        name='buff_dist',
                        datatype='GPString',
                        parameterType='Required',
                        direction='Input')

        Param2 = arcpy.Parameter(
                        displayName='Population Grid',
                        name='pop_grid',
                        datatype='DERasterDataset',
                        parameterType='Required',
                        direction='Input')

        Param3 = arcpy.Parameter(
                        displayName='Output Features',
                        name='out_feat',
                        datatype='DEFeatureClass',
                        parameterType='Required',
                        direction='Output')

        Param4 = arcpy.Parameter(
                        displayName='Output CSV',
                        name='out_csv',
                        datatype='DEFile',
                        parameterType='Derived',
                        direction='Output')

        Param0.value = 'TZ'
        Param1.value = '400 Meters'
        Param2.value = join(dirname(__file__), "Data", "Pop_Esri_TZ.tif")
        Param3.symbology = join(dirname(__file__), "Data", "Overview.lyr")
        return [Param0, Param1, Param2, Param3, Param4]

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        if arcpy.CheckExtension("Spatial") == "Available":
            return True
        else:
            return False

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def queryWPDx(self, CC):
        """Fetches all the water points from WPDx in given administrative area"""
        # First 2000 results, remove limit and get login if neccessary
        client = Socrata("data.waterpointdata.org", None)
        response = client.get("gihr-buz6", country_id = CC, limit=500000, content_type='csv')
        return response

    def getWaterPoints(self, QueryResponse):
        """Extracts points from API response"""
        with open (join(scratch, "temp.csv"), 'wb') as csvfile:
            writer = csv.writer(csvfile, delimiter='\t')
            for line in QueryResponse:
                writer.writerow(line)

        pnts = arcpy.MakeXYEventLayer_management(join(scratch, "temp.csv"),
                                                 'lon_deg', 'lat_deg', 'Temp_Layer',
                                                 spatial_reference = arcpy.SpatialReference(4326))

        return arcpy.FeatureClassToFeatureClass_conversion(pnts, scratch, "WaterPoints")


    def getPopNotServed(self, WaterPoints, PopGrid, BuffDist):
        """Extracts the population unserved by water points from population grid"""

        #Take only the functioning water points and rasterize them
        cell_size = arcpy.Describe(PopGrid).meanCellWidth
        pnts_func = arcpy.MakeFeatureLayer_management(WaterPoints, 'Functioning',
                                                      "status_id='yes'")
        start = time.clock()
        arcpy.AddMessage(BuffDist)
        pnts_buff = arcpy.Buffer_analysis(pnts_func, r"in_memory\buffer", BuffDist)    #would buffer be faster in different coordinate system?
        arcpy.AddMessage("Buffer took: {} seconds".format(time.clock()-start))
        start = time.clock()
        area_urban  = join(dirname(__file__), "Data", "ToolData.gdb", "Urban1")
        arcpy.env.extent = arcpy.Describe(WaterPoints).extent       #is too small?
        polygon_served = arcpy.Merge_management([pnts_buff, area_urban])    #results are different now!
        arcpy.AddMessage("Merge took: {} seconds".format(time.clock()-start))
        arcpy.env.snapRaster = PopGrid
        area_served = arcpy.PolygonToRaster_conversion(polygon_served, 'OBJECTID',
                                                        r"in_memory\served",
                                                        'CELL_CENTER', 'NONE',
                                                        cell_size)

        #Use Con tool to set population to 0 in raster cells that have access to water
        start = time.clock()
        area_not_served = arcpy.sa.IsNull(area_served)
        pop_not_served = arcpy.sa.Con(area_not_served, PopGrid,'0', 'Value>0')
        arcpy.AddMessage("Con took: {} seconds".format(time.clock()-start))
        return pop_not_served

    def calcUnserved(self, Admin_Zones, Unserved_Population):
        """Uses zonal statistics to calculate population unserved in each zone"""
        pop_dict = dict()
        pop_by_region = arcpy.gp.ZonalStatisticsAsTable_sa(Admin_Zones, 'ISO_CODE',
                                                          Unserved_Population,
                                                          r"in_memory\pop",
                                                           '', 'SUM')
        with arcpy.da.SearchCursor(pop_by_region, ['ISO_CODE', 'SUM' ]) as cursor:
                for row in cursor:
                    pop_dict[row[0]] = row[1]
        return pop_dict

    def outputCSV(self, Country, Points, PopDict):
        """Creates output csv file"""
        file_path = join(scratch, "{}.csv".format(Country))
        with open (file_path, 'wb') as out_csv:
            spamwriter = csv.writer(out_csv, delimiter='\t')
            for line in Points:
                site_id = line[36]
                try:
                    line.append(PopDict[site_id])
                except:
                    line.append(0)
                spamwriter.writerow(line)
        return file_path.replace('\\', '/')


    def execute(self, parameters, messages):
        """Calculates percentage of population unserved in each administrative area."""

        #Get Paramters
        global scratch
        scratch = tempfile.mkdtemp()
        country = parameters[0].valueAsText
        buff_dist = parameters[1].valueAsText
        pop_grid = parameters[2].value
        out_path = parameters[3].value
        adm_zones = join(dirname(__file__), "Data", "ToolData.gdb", "Admin")
        adm_lyr = arcpy.MakeFeatureLayer_management(adm_zones, 'Admin_Layer',
                                                  "ISO_CC='{}'".format(country))
        #Query WPDx database
        start = time.clock()
        query_response = self.queryWPDx(country)
        arcpy.AddMessage("Query took: {} seconds".format(time.clock()-start))

        start = time.clock()
        pnts = self.getWaterPoints(query_response)
        arcpy.AddMessage("Parsing query took: {} seconds".format(time.clock()-start))

        #Calculate percentage of population unserved in each administrative area
        #pnts =  arcpy.MakeFeatureLayer_management(r'D:\GETF\Test.gdb\Tanzania', "Points_Layer")
        pop_not_served = self.getPopNotServed(pnts, pop_grid, buff_dist)
        start = time.clock()
        pop_dict = self.calcUnserved(adm_lyr, pop_not_served)
        arcpy.AddMessage("Zonal stats took: {} seconds".format(time.clock()-start))

        #Append new data to output feature class
        with arcpy.da.UpdateCursor(adm_lyr, ['ISO_CODE', 'Pop_Unserved']) as cursor:
            for row in cursor:
                try:
                    row[1] = pop_dict[row[0]]
                    cursor.updateRow(row)
                except KeyError:
                    pass

        arcpy.CalculateField_management(adm_lyr, 'Percent_Served',
                                       'Round(1-[Pop_Unserved]/[Total_Pop_Esri],2)')

        parameters[3] = arcpy.CopyFeatures_management(adm_lyr, out_path)
        parameters[4].value = open(self.outputCSV(country, query_response, pop_dict))

#need better estimate of who's getting municipal delivery
#out_csv isn't working
#add parameter to exclude points with insufficient quantity
#Param2 should be a drop-down menu with aliases
#should I get a token for query?