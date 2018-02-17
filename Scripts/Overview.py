# core libraries
import arcpy
from os.path import join
from os.path import dirname
from sodapy import Socrata
import csv
import tempfile
#import cStringIO

class ServiceOverview(object):
    def __init__(self):
        """Prioritizes broken water points for repair."""
        self.label = 'Service Overview'
        self.description = 'Assesses access to water by ' + \
                           'administrative area.'
        self.canRunInBackground = True

    def getParameterInfo(self):
        """Define parameter definitions"""
        Param0 = arcpy.Parameter(
                        displayName='Country Code',
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
        Param2.value = join(dirname(__file__), "Data", "TZ_0_Pop_150.tif")
        Param3.symbology = join(dirname(__file__), "Data", "RepairPriorityEsri.lyr")
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
            spamwriter = csv.writer(csvfile, delimiter='\t')
            for line in QueryResponse:
                spamwriter.writerow(line)

        pnts = arcpy.MakeXYEventLayer_management(join(scratch, "temp.csv"),
                                                 'lon_deg', 'lat_deg', 'Temp_Layer',
                                                 spatial_reference = arcpy.SpatialReference(4326))

        return arcpy.FeatureClassToFeatureClass_conversion(pnts, scratch, "WaterPoints")


    def getPopNotServed(self, WaterPointsBuff, PopGrid):
        """Extracts the population unserved by water points from population grid"""

        #Take only the functioning water points and rasterize them
        cell_size = arcpy.Describe(PopGrid).meanCellWidth
        pnts_func = arcpy.MakeFeatureLayer_management(WaterPointsBuff, 'Functioning',
                                                      "status_id='yes'")
        area_served = arcpy.PolygonToRaster_conversion(pnts_func, 'status_id',
                                                        r"in_memory\served",
                                                        'CELL_CENTER', 'NONE',
                                                        cell_size)

        #Use Con tool to set population to 0 in raster cells that have access to water
        arcpy.env.extent = arcpy.Describe(WaterPointsBuff).extent
        area_not_served = arcpy.gp.IsNull_sa(area_served, r"in_memory\nserved")
        pop_not_served = arcpy.gp.Con_sa(area_not_served, PopGrid,
                                        r"in_memory\pop_not_served",
                                        '0', 'Value>0')
        return pop_not_served

    def calcService(self, adm_zones, pop_not_served):
        """Uses zonal statistics to calculate population unserved in each zone"""
        pop_dict = dict()
        pop_by_region = arcpy.gp.ZonalStatisticsAsTable_sa(adm_zones, 'ISO_CODE',
                                                          pop_not_served,
                                                          r"in_memory\pop",
                                                           'DATA', 'SUM')
        with arcpy.da.SearchCursor(pop_by_region, ['ISO_CODE', 'SUM' ]) as cursor:
                for row in cursor:
                    pop_dict[row[0]] = row[1]
        return pop_dict



    def outputCSV(self, Zone, Points, PopDict):
        """Creates output csv file"""
        with open (join(scratch, "{}.csv".format(Zone)), 'wb') as out_csv:
            spamwriter = csv.writer(out_csv, delimiter='\t')
            for line in Points:
                site_id = line[36]
                try:
                    line.append(PopDict[site_id])
                except:
                    line.append(0)
                spamwriter.writerow(line)
        return join(scratch, "{}.csv".format(Zone))


    def execute(self, parameters, messages):
        """The source code of the tool."""

        #Get Parameters
        global scratch
        scratch = tempfile.mkdtemp()
        country = parameters[0].valueAsText
        buff_dist = parameters[1].valueAsText
        pop_grid = parameters[2].value
        out_path = parameters[3].value
        adm_raster = join(dirname(__file__), "Data", "ToolData.gdb", "AdminRaster")
        adm_zones = arcpy.CopyFeatures_management(join(dirname(__file__), "Data", "ToolData.gdb", "Admin"),
                                                  r"in_memory\admin")
        adm_lyr = arcpy.MakeFeatureLayer_management(adm_zones, 'Admin_Layer',
                                                  "ISO_CC='{}'".format(country))


        #Calculate population not served in each administrative zone
        query_response = self.queryWPDx(country)
        pnts = self.getWaterPoints(query_response)
        pnts_buff = arcpy.Buffer_analysis(pnts, r"in_memory\buffer", buff_dist)
        pop_not_served = self.getPopNotServed(pnts_buff, pop_grid)
        pop_dict = self.calcService(adm_lyr, pop_not_served)
        arcpy.AddField_management(adm_lyr, "Pop_Unserved", "FLOAT")
        with arcpy.da.UpdateCursor(adm_lyr, ['ISO_CODE', 'Pop_Unserved']) as cursor:
            for row in cursor:
                try:
                    row[1] = pop_dict[row[0]]
                    cursor.updateRow(row)
                except KeyError:
                    pass

        output = arcpy.Project_management(adm_lyr, out_path,
                                          arcpy.SpatialReference(3857))
        parameters[3] = output
        #parameters[4] = self.outputCSV(zone, query_response, pop_dict)
        #return output

#out_csv isn't working
#add parameter to exclude points with insufficient quantity
#Param2 should be a drop-down menu with aliases
#should I get a token for query?