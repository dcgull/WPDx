# core libraries
import arcpy
from os.path import join
from os.path import dirname
from sodapy import Socrata
import csv
import tempfile
#import cStringIO

class RepairPriority(object):
    def __init__(self):
        """Prioritizes broken water points for repair."""
        self.label = 'Repair Priority'
        self.description = 'Estimates how many people are affected by each ' + \
                           'broken water point.'
        self.canRunInBackground = True

    def getParameterInfo(self):
        """Define parameter definitions"""
        Param0 = arcpy.Parameter(
                        displayName='Administrative Zone',
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
                        displayName='Output Water Points',
                        name='out_ponts',
                        datatype='DEFeatureClass',
                        parameterType='Required',
                        direction='Output')

        Param4 = arcpy.Parameter(
                        displayName='Output CSV',
                        name='out_csv',
                        datatype='DEFile',
                        parameterType='Derived',
                        direction='Output')

        Param0.value = 'Arusha'
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

    def queryWPDx(self, Zone):
        """Fetches all the water points from WPDx in given administrative area"""
        # First 2000 results, remove limit and get login if neccessary
        client = Socrata("data.waterpointdata.org", None)
        response = client.get("gihr-buz6", adm1 = Zone, limit=2000, content_type='csv')
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
        #Use a field mapping (fm) to exclude unecessary fields
        #fm = 'adm1 "adm1" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,adm1,-1,-1;adm2 "adm2" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,adm2,-1,-1;country_id "country_id" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,country_id,-1,-1;country_name "country_name" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,country_name,-1,-1;created "created" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,created,-1,-1;data_lnk "data_lnk" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,data_lnk,-1,-1;fecal_coliform_presence "fecal_coliform_presence" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,fecal_coliform_presence,-1,-1;install_year "install_year" true true false 4 Long 0 0 ,First,#,Arusha1.csv Events,install_year,-1,-1;installer "installer" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,installer,-1,-1;orig_lnk "orig_lnk" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,orig_lnk,-1,-1;pay "pay" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,pay,-1,-1;photo_lnk "photo_lnk" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,photo_lnk,-1,-1;photo_lnk_description "photo_lnk_description" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,photo_lnk_description,-1,-1;report_date "report_date" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,report_date,-1,-1;source "source" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,source,-1,-1;status "status" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,status,-1,-1;status_id "status_id" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,status_id,-1,-1;subjective_quality "subjective_quality" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,subjective_quality,-1,-1;updated "updated" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,updated,-1,-1;water_source "water_source" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,water_source,-1,-1;water_tech "water_tech" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,water_tech,-1,-1;wpdx_id "wpdx_id" true true false 8000 Text 0 0 ,First,#,Arusha1.csv Events,wpdx_id,-1,-1'
        return arcpy.FeatureClassToFeatureClass_conversion(pnts, 'in_memory', 'pnts')#, "", fm)

    def getPopNotServed(self, WaterPointsBuff, PopGrid):
        """Extracts the population unserved by water points from population grid"""

        #Take only the functioning water points and rasterize them
        cell_size = arcpy.Describe(PopGrid).meanCellWidth
        pnts_func = arcpy.MakeFeatureLayer_management(WaterPointsBuff, 'Functioning', "status_id='yes'")
        area_served = arcpy.PolygonToRaster_conversion(pnts_func, 'status_id', r"in_memory\served", 'CELL_CENTER', 'NONE', cell_size)

        #Use Con tool to set population to 0 in raster cells that have access to water
        arcpy.env.extent = arcpy.Describe(WaterPointsBuff).extent
        area_not_served = arcpy.gp.IsNull_sa(area_served, r"in_memory\nserved")
        pop_not_served = arcpy.gp.Con_sa(area_not_served, PopGrid, r"in_memory\pop_not_served", '0', 'Value>0')
        return pop_not_served

    def calcPriority(self, Points, PopGrid):
        """Uses zonal statistics to calculate population served by each point"""
        #incr_pop = arcpy.gp.ZonalStatisticsAsTable_sa(pnts_nonfunc, 'wpdx_id', pop_not_served, r"in_memory\incr_pop", 'DATA', 'SUM')

        #create list of non-functioning points
        pnts = list()
        pnts_nonfunc = arcpy.MakeFeatureLayer_management(Points, 'NonFunctioning', "status_id='no'")
        with arcpy.da.SearchCursor(pnts_nonfunc, 'wpdx_id') as cursor:
            for row in cursor:
                pnts.append(row[0])

        #create dictionary with population served by each point
        pop_dict = dict()
        for pnt in pnts:
            pnt_id = pnt.split('-')[1]
            point = arcpy.MakeFeatureLayer_management(pnts_nonfunc, pnt, "wpdx_id='{}'".format(pnt))
            incr_pop = arcpy.gp.ZonalStatisticsAsTable_sa(point, 'wpdx_id', PopGrid, r"in_memory\pop{}".format(pnt_id), 'DATA', 'SUM')
            with arcpy.da.SearchCursor(incr_pop, ['wpdx_id', 'SUM' ]) as cursor:
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
        zone = parameters[0].valueAsText
        buff_dist = parameters[1].valueAsText
        pop_grid = parameters[2].value
        out_path = parameters[3].value

        #Calculate incremental population that could be served by each broken water point
        query_response = self.queryWPDx(zone)
        pnts = self.getWaterPoints(query_response)
        pnts_buff = arcpy.Buffer_analysis(pnts, r"in_memory\buffer", buff_dist)
        pop_not_served = self.getPopNotServed(pnts_buff, pop_grid)
        pop_dict = self.calcPriority(pnts_buff, pop_not_served)


        #Add population served to water points as an attribute
        pnts_nonfunc = arcpy.MakeFeatureLayer_management(pnts, 'NonFunctioning', "status_id='no'")
        arcpy.AddField_management(pnts_nonfunc, "Pop_Served", "FLOAT")
        with arcpy.da.UpdateCursor(pnts_nonfunc, ['wpdx_id', 'Pop_Served']) as cursor:
            for row in cursor:
                try:
                    row[1] = pop_dict[row[0]]
                    cursor.updateRow(row)
                except KeyError:
                    pass

        output = arcpy.Project_management(pnts_nonfunc, out_path, arcpy.SpatialReference(3857))
        parameters[3] = output
        parameters[4] = self.outputCSV(zone, query_response, pop_dict)
        #return output

#out_csv isn't working
#add parameter to exclude points with insufficient quantity
#Param2 should be a drop-down menu with aliases
#sould I get a token for query?