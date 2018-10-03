# -------------------------------------------------------------------------------
# Name:        WPDx Decision Support Toolset
# Purpose:     Tools for working with the Water Point Data Exchange
# Author:      Daniel Siegel, Esri
# Created:     2018-01-04
# -------------------------------------------------------------------------------


# core libraries
import arcpy
from os.path import join
from os.path import dirname
from sodapy import Socrata
import csv
import tempfile
import json
import sys
dacc = arcpy.da

fc_adm_zones = r"C:\Users\doug6376\Documents\WPDx-Toolset\Data\ToolData.gdb\Admin"
fc_area_urban = r"C:\Users\doug6376\Documents\WPDx-Toolset\Data\ToolData.gdb\Urban"
lyr_new_locations = r"C:\Users\doug6376\Documents\WPDx-Toolset\Data\NewLocations.lyr"
lyr_overview = r"C:\Users\doug6376\Documents\WPDx-Toolset\Data\Overview.lyr"
lyr_repair_priority_esri = r"C:\Users\doug6376\Documents\WPDx-Toolset\Data\RepairPriorityEsri.lyr"
md_population_sources = r"C:\Users\doug6376\Documents\WPDx-Toolset\Data\ToolData.gdb\POPULATION_SOURCES"

class Toolbox(object):
    def __init__(self):
        """Tools for working with the Water Point Data Exchange"""
        self.label = "WPDx Decision Support Toolset"
        self.alias = ""
        self.tools = [RepairPriority, ServiceOverview, NewLocations, SeePopNotServed, UpdatePop]
        self.dict_population_sources = {}


# make sure to install these packages before running:
# pip install sodapy

# useful doc is here:
# https://dev.socrata.com/foundry/data.waterpointdata.org/gihr-buz6
# https://github.com/xmunoz/sodapy#getdataset_identifier-content_typejson-kwargs

# <editor-fold desc="Core Functions">####################################################################################
#                                            Core Functions
########################################################################################################################

def get_all_image_sources():
    with dacc.SearchCursor(md_population_sources, ["Name", "Raster", "LowPS"]) as sc:
        return dict([(row[0], {"Raster": row[1], "LowPS": row[2]}) for row in sc])

def setEnvironment(zone, query_type):
    """Limits the processing extent to the given administrative zone"""
    mask = arcpy.FeatureClassToFeatureClass_conversion(fc_adm_zones, "in_memory", "mask", "{0}='{1}'".format(query_type, zone))
    if mask.maxSeverity==1:
        arcpy.AddError("ERROR: Database error in {}. Please alert system administrator".format(zone))
        sys.exit(1)
    else:
        extent = arcpy.Describe(mask).extent
        arcpy.env.extent = extent
        #arcpy.env.mask = mask
        return mask

def queryWPDx(zone):
    """Fetches all the water points from WPDx in given administrative area"""
    # First 2000 results, remove limit and get login if neccessary
    start = time.clock()
    client = Socrata("data.waterpointdata.org", "DxaZOVlLSiCqe5fPAI08cI4qM")
    # Set output fields (this only affects the Repair Priority tool)
    fields = 'adm1,adm2,country_id,country_name,created,data_lnk,fecal_coliform_presence,install_year,installer,photo_lnk,photo_lnk_description,report_date,source,status_id,subjective_quality,updated,water_source,water_tech,wpdx_id,lat_deg,lon_deg'

    zone1 = zone
    # it is better to change the'Admin' dataset to match what WPDx API is expecting,
    # but if you do want to allow multiple spellings of the same word, do it here.
    if zone1.lower() == 'tanzania':
        zone1 = 'Tanzania, United Republic of'
    # All admin areas in Swazliand must be in ALL CAPS

    if len(zone) > 2:
        response = client.get("gihr-buz6", adm1=zone1, limit=50000, select= fields)
        if len(response) > 1:
            query_type = 'Admin1'
        else:
            response = client.get("gihr-buz6", adm2=zone1, limit=50000, select= fields)
            if len(response) > 1:
                query_type = 'Name'
            else:
                response = client.get("gihr-buz6", country_name=zone1, limit=500000, select= fields)
                if len(response) > 1:
                    query_type = 'Country'
                else:
                    arcpy.AddError("ERROR: Administrative zone not recognized")
                    # Ambiguous error, this can also mean the zone is recognized
                    # but has no points in it
                    sys.exit(1)
    else:
        response = client.get("gihr-buz6", country_id=zone1, limit=500000, select= fields)
        if len(response) > 1:
            query_type = 'cc'
        else:
            arcpy.AddError("ERROR: Country Code not recognized")
            sys.exit(1)

    arcpy.AddMessage("Query took: {:.2f} seconds".format(time.clock() - start))
    arcpy.AddMessage("Found: {} points".format(len(response)))
    mask = setEnvironment(zone, query_type)
    return (response, mask)


def getWaterPoints(query_response, hide_fields=False):
    """Extracts points from API response"""
    start = time.clock()
    keys = set()
    for line in query_response:
        keys.update(line.keys())
    with open(join(scratch, "temp.csv"), 'w') as csvfile:
        writer = csv.DictWriter(csvfile, keys, delimiter='\t')
        writer.writeheader()
        for line in query_response:
            try:
                writer.writerow(line)
            except UnicodeEncodeError:
                arcpy.AddMessage("Row droppped due to invalid characters: {}".format(line['wpdx_id']))
                continue
    pnts = arcpy.MakeXYEventLayer_management(join(scratch, "temp.csv"), 'lon_deg', 'lat_deg', 'Temp_Layer', spatial_reference=arcpy.SpatialReference(4326))

    arcpy.AddMessage("Parsing query took: {:.2f} seconds".format(time.clock() - start))
    return arcpy.FeatureClassToFeatureClass_conversion(pnts, scratch, "WaterPoints")


def getPopNotServed(water_points_buff, pop_grid, urban_area=None):
    """Extracts the population unserved by water points from population grid"""

    # Get path to population data
    path = Toolbox.dict_population_sources[pop_grid]["Raster"]
    cell_size = Toolbox.dict_population_sources[pop_grid]["LowPS"]

    try:
        arcpy.AddMessage("Cell Size: {}".format(cell_size))
    except:
        arcpy.AddError("ERROR: Path to {} population data is incorrect".format(pop_grid))
        sys.exit(1)


    # arcpy.env.snapRaster = pop_grid
    # need a way to extract the correct item from mosaic dataset instead of using mosaic itself as snap raster

    # filter out urban areas where water points aren't necessary
    if urban_area:
        start = time.clock()
        polygon_served = arcpy.Merge_management([water_points_buff, urban_area],
                                                r"in_memory\served_poly")  # results are different now! compare to 2x con method
        arcpy.AddMessage("Merge took: {:.2f} seconds".format(time.clock() - start))
    else:
        polygon_served = water_points_buff

    # arcpy.env.snapRaster = pop_grid
    oid = [f.name for f in arcpy.Describe(polygon_served).fields][0]
    area_served = arcpy.PolygonToRaster_conversion(polygon_served, oid, r"in_memory\served", 'CELL_CENTER', 'NONE',
                                                   cell_size)
    # add a better error for when the extent is too big for memory
    # arcpy.env.snapRaster = area_served

    # Use Con tool to set population to 0 in raster cells that have access to water
    start = time.clock()
    area_not_served = arcpy.sa.IsNull(area_served)#, r"in_memory\not_served")
    pop_not_served = arcpy.sa.Con(area_not_served, path, '0', 'Value > 0')
    arcpy.AddMessage("Con took: {:.2f} seconds".format(time.clock() - start))
    return pop_not_served


# </editor-fold>###########################################################################
#                            Tools
############################################################################################

class NewLocations(object):
    def __init__(self):
        """Finds optimal locations for new water points."""
        self.label = 'New Locations'
        self.description = 'Finds optimal locations for new water points ' + \
                           'that maximize population served.'
        self.canRunInBackground = True

    def execute(self, parameters, messages):
        """Calculates percentage of population unserved in each administrative area."""
        #scratchworkspace = "in_memory"

        # Get Paramters
        global scratch
        scratch = tempfile.mkdtemp()
        zone = parameters[0].valueAsText
        num = parameters[1].valueAsText
        buff_dist = parameters[2].valueAsText
        pop_grid = parameters[3].value
        out_path = parameters[4].value
        Toolbox.dict_population_sources = get_all_image_sources()

        # Query WPDx database
        query_response, mask = queryWPDx(zone)
        pnts = getWaterPoints(query_response)

        start = time.clock()
        pnts_func = arcpy.MakeFeatureLayer_management(pnts, 'Functioning',
                                                      "status_id='yes'")

        pnts_buff = arcpy.Buffer_analysis(pnts_func, r"in_memory\buffer", "{} Meters".format(buff_dist))
        if mask == "Error":
            arcpy.env.extent = arcpy.Describe(pnts_buff).extent
        arcpy.AddMessage("Buffer took: {:.2f} seconds".format(time.clock() - start))

        pop_not_served = getPopNotServed(pnts_buff, pop_grid, fc_area_urban)


        cell_size = float(Toolbox.dict_population_sources[pop_grid]["LowPS"])*111000

        cell_factor = int(round(float(buff_dist) / cell_size))
        neighborhood = arcpy.sa.NbrCircle(cell_factor, "CELL")

        FocalStat = arcpy.sa.FocalStatistics(pop_not_served, neighborhood,
                                     "SUM", "DATA")

        agg = arcpy.sa.Aggregate(FocalStat, cell_factor, 'MAXIMUM')
        #arcpy.env.mask = mask
        agg_pnts = arcpy.RasterToPoint_conversion(agg, r"in_memory\agg_pnt", 'Value')
        sort = arcpy.Sort_management(agg_pnts, r"in_memory\sort", 'grid_code DESCENDING')
        top = arcpy.MakeFeatureLayer_management(sort, 'TOP', "OBJECTID<{}".format(num))
        arcpy.AlterField_management (top, "grid_code", "Pop_Served", "Pop_Served")
        output = arcpy.CopyFeatures_management(top, join(arcpy.env.scratchGDB, "NewLocations")).getOutput(0)

        parameters[4].value = output
        parameters[5].value = self.outputCSV(output, zone)

        # should zones close to broken points count as good locations for a new installation?



    def outputCSV(self, fc, zone):
        """Creates output csv file"""
        arcpy.AddField_management(fc, 'Longitude', 'FLOAT')
        arcpy.AddField_management(fc, 'Latitude', 'FLOAT')
        arcpy.CalculateField_management(fc, 'Latitude', '!SHAPE!.firstPoint.Y', 'PYTHON_9.3')
        arcpy.CalculateField_management(fc, 'Longitude', '!SHAPE!.firstPoint.X', 'PYTHON_9.3')

        fields = [field.name for field in arcpy.Describe(fc).fields]
        fields.remove('pointid'); fields.remove('Shape')
        #file_path = join(scratch, "{}_NewLocations.csv".format(zone))
        file_path = join(arcpy.env.scratchFolder, "{}_NewLocations.csv".format(zone))
        with open(file_path, 'w') as out_csv:
            writer = csv.writer(out_csv, delimiter='\t')
            writer.writerow(fields)
            with arcpy.da.SearchCursor(fc, fields) as rows:
                for row in rows:
                    writer.writerow(row)
        return file_path

    def getParameterInfo(self):
        """Define parameter definitions"""
        Param0 = arcpy.Parameter(
            displayName='Administrative Zone',
            name='zone',
            datatype='GPString',
            parameterType='Required',
            direction='Input')

        Param1 = arcpy.Parameter(
            displayName='Number of Candidates',
            name='number',
            datatype='GPString',
            parameterType='Required',
            direction='Input')

        Param2 = arcpy.Parameter(
            displayName='Access Distance (meters)',
            name='buff_dist',
            datatype='GPString',
            parameterType='Required',
            direction='Input')

        Param3 = arcpy.Parameter(
            displayName='Population Grid',
            name='pop_grid',
            datatype='GPString',
            parameterType='Required',
            direction='Input')

        Param4 = arcpy.Parameter(
            displayName='Output Features',
            name='out_feat',
            datatype='DEFeatureClass',
            parameterType='Derived',
            direction='Output')

        Param5 = arcpy.Parameter(
            displayName='Output CSV',
            name='out_csv',
            datatype='DEFile',
            parameterType='Derived',
            direction='Output')

        Param0.value = 'Arusha'
        Param1.value = '100'
        Param2.value = '1000'
        Param3.filter.type = 'ValueList'
        Param3.filter.list = get_all_image_sources().keys()
        Param3.value = Param3.filter.list[0]
        Param4.symbology = lyr_new_locations
        Param4.value = r"in_memory\NewLocations"

        return [Param0, Param1, Param2, Param3, Param4, Param5]

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


class RepairPriority(object):
    def __init__(self):
        """Prioritizes broken water points for repair."""
        self.label = 'Repair Priority'
        self.description = 'Estimates how many people are affected by each ' + \
                           'broken water point.'
        self.canRunInBackground = True


    def calcPriority(self, pnts_buff, pop_grid):
        """Uses zonal statistics to calculate population served by each point"""

        # create list of non-functioning points
        pnts = list()
        with arcpy.da.SearchCursor(pnts_buff, 'wpdx_id', "status_id='no'") as cursor:
            for row in cursor:
                pnts.append(row[0])

        # create dictionary with population served by each point
        start = time.clock()
        pop_dict = dict()

        # Code is commented out bc ZonalStatisticsAsTable doesn't currently work with overlapping polygons
        # incr_pop = arcpy.gp.ZonalStatisticsAsTable_sa(pnts_nonfunc, 'wpdx_id',
        #                                              pop_grid,
        #                                                  r"in_memory\pop",
        #                                                   'DATA', 'SUM')
        #
        # with arcpy.da.SearchCursor(incr_pop, ['wpdx_id', 'SUM' ]) as cursor:
        #    for row in cursor:
        #        pop_dict[row[0]] = row[1]

        # Bellow is a workaround. Delete once bug from line 353 is fixed
        ####################################################################
        # why does this take 100 s more than same code in old toolbox?
        for pnt in pnts:
            pnt_id = pnt.split('-')[1]
            point = arcpy.MakeFeatureLayer_management(pnts_buff, pnt,
                                                      "wpdx_id='{}'".format(pnt))
            incr_pop = arcpy.gp.ZonalStatisticsAsTable_sa(point, 'wpdx_id',
                                                          pop_grid,
                                                          r"in_memory\pop{}".format(pnt_id),
                                                          'DATA', 'SUM')
            with arcpy.da.SearchCursor(incr_pop, ['wpdx_id', 'SUM']) as cursor:
                for row in cursor:
                    pop_dict[row[0]] = row[1]
        #############################################################################

        arcpy.AddMessage("Zonal Stats took: {:.2f} seconds".format(time.clock() - start))
        return pop_dict

    def outputCSV(self, zone, points, pop_dict):
        """Creates output csv file"""
        keys = set()
        keys.add("Pop_Served")
        for line in points:
            keys.update(line.keys())
        arcpy.AddMessage(keys)
        file_path = join(arcpy.env.scratchFolder, "{}_RepairPriority.csv".format(zone))
        with open(file_path, 'wb') as out_csv:
            writer = csv.DictWriter(out_csv, keys, delimiter='\t')
            writer.writeheader()
            for line in points:

                if line['status_id'] == 'yes':
                    continue
                site_id = line['wpdx_id']
                try:
                    line['Pop_Served'] = pop_dict[site_id]
                except:
                    line['Pop_Served'] = 0
                writer.writerow(line)
        return file_path

    def execute(self, parameters, messages):
        """The source code of the tool."""

        # Get Parameters
        global scratch
        scratch = tempfile.mkdtemp()
        zone = parameters[0].valueAsText
        buff_dist = parameters[1].valueAsText
        pop_grid = parameters[2].value
        out_path = parameters[3].value
        Toolbox.dict_population_sources = get_all_image_sources()

        # Calculate incremental population that could be served by each broken water point
        query_response, mask = queryWPDx(zone)
        pnts = getWaterPoints(query_response)

        start = time.clock()
        pnts_buff = arcpy.Buffer_analysis(pnts, r"in_memory\buffer", "{} Meters".format(buff_dist))
        if mask == "Error":
            arcpy.env.extent = arcpy.Describe(pnts_buff).extent
        arcpy.AddMessage("Buffer took: {:.2f} seconds".format(time.clock() - start))
        pnts_buff_func = arcpy.MakeFeatureLayer_management(pnts_buff, 'Functioning',
                                                           "status_id='yes'")
        pop_not_served = getPopNotServed(pnts_buff_func, pop_grid)

        # Add population served to water points as an attribute
        pop_dict = self.calcPriority(pnts_buff, pop_not_served)
        arcpy.AddField_management(pnts, "Pop_Served", "FLOAT")
        pnts_nonfunc = arcpy.MakeFeatureLayer_management(pnts, 'NonFunctioning',
                                                         "status_id='no'")
        with arcpy.da.UpdateCursor(pnts_nonfunc, ['wpdx_id', 'Pop_Served']) as cursor:
            for row in cursor:
                try:
                    row[1] = pop_dict[row[0]]
                    cursor.updateRow(row)
                except KeyError:
                    pass

        output = arcpy.CopyFeatures_management(pnts_nonfunc, join(arcpy.env.scratchGDB, "RepairPriority")).getOutput(0)

        parameters[3].value = output
        parameters[4].value = self.outputCSV(zone, query_response, pop_dict)

    def getParameterInfo(self):
        """Define parameter definitions"""
        Param0 = arcpy.Parameter(
            displayName='Administrative Zone',
            name='zone',
            datatype='GPString',
            parameterType='Required',
            direction='Input')

        Param1 = arcpy.Parameter(
            displayName='Access Distance (meters)',
            name='buff_dist',
            datatype='GPString',
            parameterType='Required',
            direction='Input')

        Param2 = arcpy.Parameter(
            displayName='Population Grid',
            name='pop_grid',
            datatype='GPString',
            parameterType='Required',
            direction='Input')

        Param3 = arcpy.Parameter(
            displayName='Output Water Points',
            name='out_ponts',
            datatype='DEFeatureClass',
            parameterType='Derived',
            direction='Output')

        Param4 = arcpy.Parameter(
            displayName='Output CSV',
            name='out_csv',
            datatype='DEFile',
            parameterType='Derived',
            direction='Output')

        Param0.value = 'Arusha'
        Param1.value = '1000'
        Param2.filter.type = 'ValueList'
        Param2.filter.list = get_all_image_sources().keys()
        Param2.value = Param2.filter.list[0]
        Param3.symbology = lyr_repair_priority_esri
        Param3.value = r"in_memory\RepairPriority"
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


class ServiceOverview(object):
    def __init__(self):
        """Estimates access to safe water by administrative area."""
        self.label = 'Service Overview'
        self.description = 'Estimates access to safe water by ' + \
                           'administrative area.'
        self.canRunInBackground = True

    def calcUnserved(self, admin_zones, unserved_population):
        """Uses zonal statistics to calculate population unserved in each zone"""
        start = time.clock()
        pop_dict = dict()
        pop_by_region = arcpy.gp.ZonalStatisticsAsTable_sa(admin_zones, 'Name',
                                                           unserved_population,
                                                           r"in_memory\pop",
                                                           '', 'SUM')
        with arcpy.da.SearchCursor(pop_by_region, ['Name', 'SUM']) as cursor:
            for row in cursor:
                pop_dict[row[0]] = row[1]
        arcpy.AddMessage("Zonal stats took: {:.2f} seconds".format(time.clock() - start))
        return pop_dict

    def outputCSV(self, Country, fc):
        """Creates output csv file"""
        fields = [field.name for field in arcpy.Describe(fc).fields]
        fields.remove('Rural_Pop_Esri'); fields.remove('Rural_Pop_Worldpop'); fields.remove('Shape')
        file_path = join(scratch, "{}_ServiceOverview.csv".format(Country))
        with open(file_path, 'wb') as out_csv:
            writer = csv.writer(out_csv, delimiter='\t')
            writer.writerow(fields)
            with arcpy.da.SearchCursor(fc, fields) as rows:
                for row in rows:
                    writer.writerow(row)
        return file_path

    def execute(self, parameters, messages):
        """Calculates percentage of population unserved in each administrative area."""

        # Get Paramters
        global scratch
        scratch = tempfile.mkdtemp()
        country = parameters[0].valueAsText
        buff_dist = parameters[1].valueAsText
        pop_grid = parameters[2].value
        out_path = "in_memory\ServiceOverview"

        # Query WPDx database
        query_response, mask = queryWPDx(country)
        if mask == "Error":
            sys.exit(1)
        pnts = getWaterPoints(query_response)

        # Calculate percentage of population unserved in each administrative area
        start = time.clock()
        pnts_func = arcpy.MakeFeatureLayer_management(pnts, 'Functioning',
                                                      "status_id='yes'")
        pnts_buff = arcpy.Buffer_analysis(pnts_func, r"in_memory\buffer", "{} Meters".format(buff_dist))
                       # would buffer be faster in different coordinate system?
        arcpy.AddMessage("Buffer took: {:.2f} seconds".format(time.clock() - start))

        pop_not_served = getPopNotServed(pnts_buff, pop_grid, fc_area_urban)
        pop_dict = self.calcUnserved(mask, pop_not_served)
        output = arcpy.CopyFeatures_management(mask, out_path)

        # Append new data to output feature class
        with arcpy.da.UpdateCursor(output, ['Name', 'Pop_Unserved']) as cursor:
            for row in cursor:
                try:
                    row[1] = pop_dict[row[0]]
                except KeyError:
                    row[1] = 0
                    # this means if we have no population data for a region, it gets 100% served (not ideal)
                    # if Rural_Pop is not pre-calculated, Percent_Served = 0. Would be better to throw an error.
                cursor.updateRow(row)
        arcpy.CalculateField_management(output, 'Percent_Served',
                                        'round(1-!Pop_Unserved!/!Rural_Pop_{}!,2)'.format(pop_grid),
                                        'Python')
        arcpy.CalculateField_management(output, 'Percent_Served', 'max(0, !Percent_Served!)', 'PYTHON_9.3')

        parameters[3] = output
        parameters[4].value = self.outputCSV(country, output)


    def getParameterInfo(self):
        """Define parameter definitions"""
        Param0 = arcpy.Parameter(
            displayName='Country',
            name='zone',
            datatype='GPString',
            parameterType='Required',
            direction='Input')

        Param1 = arcpy.Parameter(
            displayName='Access Distance (meters)',
            name='buff_dist',
            datatype='GPString',
            parameterType='Required',
            direction='Input')

        Param2 = arcpy.Parameter(
            displayName='Population Grid',
            name='pop_grid',
            datatype='GPString',
            parameterType='Required',
            direction='Input')

        Param3 = arcpy.Parameter(
            displayName='Output Features',
            name='out_feat',
            datatype='DEFeatureClass',
            parameterType='Derived',
            direction='Output')

        Param4 = arcpy.Parameter(
            displayName='Output CSV',
            name='out_csv',
            datatype='DEFile',
            parameterType='Derived',
            direction='Output')

        Param0.value = 'TZ'
        Param1.value = '1000'
        Param2.value = 'Worldpop'
        Param2.filter.type = 'ValueList'
        Param2.filter.list = ['Esri', 'Worldpop']
        Param3.symbology = lyr_overview
        Param3.value = "in_memory\ServiceOverview"
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

class SeePopNotServed(object):
    def __init__(self):
        """Creates map for any administrative area of population not served."""
        self.label = 'See Unserved Population'
        self.description = 'See the distribution of unserved population  ' + \
                           'in a given administrative area.'
        self.canRunInBackground = True
        self.category = "Utilities"

    def execute(self, parameters, messages):
        """Removes urban areas and areas near a functioning well from population raster."""

        # Get Paramters
        global scratch
        scratch = tempfile.mkdtemp()
        zone = parameters[0].valueAsText
        buff_dist = parameters[1].valueAsText
        pop_grid = parameters[2].value
        out_path = parameters[3].value

        # Query WPDx database
        query_response, mask = queryWPDx(zone)
        pnts = getWaterPoints(query_response)

        start = time.clock()
        pnts_func = arcpy.MakeFeatureLayer_management(pnts, 'Functioning',
                                                      "status_id='yes'")

        pnts_buff = arcpy.Buffer_analysis(pnts_func, r"in_memory\buffer", "{} Meters".format(buff_dist))
        if mask == "Error":
            arcpy.env.extent = arcpy.Describe(pnts_buff).extent
        arcpy.AddMessage("Buffer took: {:.2f} seconds".format(time.clock() - start))

        pop_not_served = getPopNotServed(pnts_buff, pop_grid, fc_area_urban)

        output = arcpy.CopyRaster_management(pop_not_served, out_path)
        parameters[3] = output

    def getParameterInfo(self):
        """Define parameter definitions"""
        Param0 = arcpy.Parameter(
            displayName='Administrative Zone',
            name='zone',
            datatype='GPString',
            parameterType='Required',
            direction='Input')

        Param1 = arcpy.Parameter(
            displayName='Access Distance (meters)',
            name='buff_dist',
            datatype='GPString',
            parameterType='Required',
            direction='Input')

        Param2 = arcpy.Parameter(
            displayName='Population Grid',
            name='pop_grid',
            datatype='GPString',
            parameterType='Required',
            direction='Input')

        Param3 = arcpy.Parameter(
            displayName='Output Features',
            name='out_feat',
            datatype='DERasterDataset',
            parameterType='Derived',
            direction='Output')

        Param0.value = 'Arusha'
        Param1.value = '1000'
        Param2.value = 'Worldpop'
        Param2.filter.type = 'ValueList'
        Param2.filter.list = ['Esri', 'Worldpop']
        Param3.value = "in_memory\PopNotServed"
        # Param4.symbology = join(dirname(__file__), "Data", "RepairPriorityEsri.lyr")
        return [Param0, Param1, Param2, Param3]

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

class UpdatePop(object):
    def __init__(self):
        """Finds optimal locations for new water points."""
        self.label = 'Update Rural Population'
        self.description = 'Recalculates the rurl population of each   ' + \
                           'administrative zone. Use when population data or ' + \
                           'urban area extents are updated.'
        self.canRunInBackground = True
        self.category = "Utilities"

    def execute(self, parameters, messages):
        """Calculates rural population in each administrative area."""

        #set up a scratch workspace and set it as env
        scratch = tempfile.mkdtemp()
        gdb = arcpy.CreateFileGDB_management(scratch, "temp").getOutput(0)
        #arcpy.env.scratchGDB = gdb
        #arcpy.env.workspace = gdb
        #arcpy.env.scratchWorkspace = gdb
        country = parameters[0].valueAsText

        if len(country) > 2:
            query_type = 'Country'
        else:
            query_type = 'cc'
        mask = setEnvironment(country, query_type)


        # arcpy.env.snapRaster = pop_grid
        cell_size = '0.0008333'
        start = time.clock()
        area_served = arcpy.PolygonToRaster_conversion(fc_area_urban,
                                                       'RANK',
                                                       join(gdb, 'served'),
                                                       'CELL_CENTER', 'NONE',
                                                       cell_size)


        area_not_served = arcpy.gp.IsNull  (area_served)
        arcpy.AddMessage("Rasterize took: {:.2f} seconds".format(time.clock() - start))
        start = time.clock()

        Toolbox.dict_population_sources = get_all_image_sources()

        for name in Toolbox.dict_population_sources:
            #try:
            pop_grid = Toolbox.dict_population_sources[name]["Raster"]

            pop_not_served = arcpy.sa.Con(area_not_served, pop_grid, '0', 'Value>0')
            arcpy.AddMessage("Con took: {:.2f} seconds".format(time.clock() - start))

            start = time.clock()
            pop_by_region = arcpy.gp.ZonalStatisticsAsTable_sa(fc_adm_zones, 'Name', pop_not_served, r"in_memory\pop{}".format(name), '', 'SUM')
            arcpy.AddMessage("Zonal Stats took: {:.2f} seconds".format(time.clock() - start))
            pop_dict = dict()
            with arcpy.da.SearchCursor(pop_by_region, ['Name', 'SUM']) as cursor:
                for row in cursor:
                    pop_dict[row[0]] = row[1]

            with arcpy.da.UpdateCursor(fc_adm_zones, ['Name', 'Rural_Pop_{}'.format(name)], "{} = '{}'".format(query_type, country)) as cursor:
                for row in cursor:
                    try:
                        row[1] = pop_dict[row[0]]
                        cursor.updateRow(row)
                    except KeyError:
                        pass
            #except:
             #   arcpy.AddError("ERROR: No {} data available in this region".format(name))


    def getParameterInfo(self):
        """Define parameter definitions"""
        Param0 = arcpy.Parameter(
            displayName='Country',
            name='country',
            datatype='GPString',
            parameterType='Required',
            direction='Input')
        Param0.value = 'Swaziland'
        return [Param0]

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

