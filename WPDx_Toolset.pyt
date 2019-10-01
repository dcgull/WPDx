# -------------------------------------------------------------------------------
# Name:        WPDx Decision Support Toolset
# Purpose:     Tools for working with the Water Point Data Exchange
# Author:      Daniel Siegel, Esri
# Created:     2018-01-04
# -------------------------------------------------------------------------------

# core libraries
import arcpy
import requests
import pandas as pd
from os.path import join
from os.path import dirname
from sodapy import Socrata
import csv
import tempfile
import json
import sys
dacc = arcpy.da
import time
import os

# loc = r"C:\Users\esri\Desktop\Archive"
loc = os.path.dirname(os.path.abspath(__file__))  # set wd to the folder that contains this script
fc_adm_zones = "{}\\Data\\ToolData.gdb\\Admin".format(loc)
fc_area_urban = "{}\\Data\\ToolData.gdb\\Urban".format(loc)
lyr_new_locations = "{}\\Data\\NewLocations.lyr".format(loc)
lyr_popnotserved = "{}\\Data\\PopNotServed.lyr".format(loc)
lyr_overview = "{}\\Data\\Overview.lyr".format(loc)
lyr_repair_priority_esri = "{}\\Data\\RepairPriorityEsri.lyr".format(loc)
md_population_sources = "{}\\Data\\ToolData.gdb\\WORLD_POPULATION1".format(loc)


class Toolbox(object):
    def __init__(self):
        """Tools for working with the Water Point Data Exchange"""
        self.label = "WPDx Decision Support Toolset"
        self.alias = ""
        self.tools = [
            RepairPriority, ServiceOverview, NewLocations, SeePopNotServed,
            UpdatePop, UrbanThreshold, UpdateDatabase
        ]
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
    with dacc.SearchCursor(md_population_sources,
                           ["Name", "Raster", "LowPS"]) as sc:
        return dict([(row[0], {
            "Raster": row[1],
            "LowPS": row[2]
        }) for row in sc])


def setEnvironment(zone):
    """Limits the processing extent to the given administrative zone"""

    # arcpy overwrite output
    arcpy.env.overwriteOutput = True

    mask = arcpy.FeatureClassToFeatureClass_conversion(
            fc_adm_zones, arcpy.env.scratchGDB, "mask", "{0}='{1}'".format(
            'CC', zone.upper()))
    if mask.maxSeverity == 1:
        for qt in ['Admin1', 'Name', 'Country']:
            mask = arcpy.FeatureClassToFeatureClass_conversion(
                fc_adm_zones, arcpy.env.scratchGDB, "mask", "{0}='{1}'".format(
                    qt, zone))
            if mask.maxSeverity == 0:
                break
            mask = arcpy.FeatureClassToFeatureClass_conversion(
                fc_adm_zones, arcpy.env.scratchGDB, "mask", "{0}='{1}'".format(
                    qt, zone.title()))
            if mask.maxSeverity == 0:
                break
        else:
            arcpy.AddError(
                "ERROR: No administrative zone called - {} - in database.".
                format(zone))
            sys.exit(1)

    with arcpy.da.SearchCursor(mask, 'CC') as cursor:
        for result in cursor:
            country_code = result[0]

    extent = arcpy.Describe(mask).extent
    arcpy.env.extent = extent
    #arcpy.env.mask = mask
    return mask, country_code


def queryWPDx(country_code=None):
    """Fetches all the water points from WPDx in given administrative area"""

    start = time.clock()
    with Socrata("data.waterpointdata.org", "DxaZOVlLSiCqe5fPAI08cI4qM") as client:
        if country_code:
            response = client.get(
                "gihr-buz6",
                country_id=country_code.upper(),
                limit=1000000)
            arcpy.AddMessage("Found: {} points in this country".format(len(response)))
        else:
            response = client.get(
                "gihr-buz6",
                 limit=50000000)
            arcpy.AddMessage("Found: {} points in the database".format(len(response)))
        if len(response) < 2:
            arcpy.AddError("ERROR: Country Code not recognized")
            sys.exit(1)

    arcpy.AddMessage("Query took: {:.2f} seconds".format(time.clock() - start))
    return response


def getWaterPoints(query_response, hide_fields=False):
    """Extracts points from API response"""
    start = time.clock()
    df = pd.DataFrame(query_response)
    df1 = df.sort_values('updated', ascending=False)
    df2 = df1.drop_duplicates('wpdx_id')
    df2.to_csv(join(arcpy.env.scratchFolder, "temp.csv"), encoding = 'utf-8')

    """
    keys = set()
    for line in query_response:
        keys.update(line.keys())
    ordered_keys = sorted(keys)pnts = arcpy.MakeXYEventLayer_management(
        join(arcpy.env.scratchFolder, "temp.csv"),
        'lon_deg',
        'lat_deg',
        'Temp_Layer',
        spatial_reference=arcpy.SpatialReference(4326))
    with open(join(arcpy.env.scratchFolder, "temp.csv"), 'w') as csvfile:
        writer = csv.DictWriter(csvfile, ordered_keys)
        writer.writeheader()
        for line in query_response:
            try:
                writer.writerow(line)
            except UnicodeEncodeError:
                arcpy.AddMessage(
                    "Water point droppped due to invalid formatting: {}".
                    format(line['wpdx_id']))
                continue
    """
    pnts = arcpy.MakeXYEventLayer_management(
        join(arcpy.env.scratchFolder, "temp.csv"),
        'lon_deg',
        'lat_deg',
        'Temp_Layer',
        spatial_reference=arcpy.SpatialReference(4326))

    arcpy.AddMessage(
        "Parsing query took: {:.2f} seconds".format(time.clock() - start))
    return arcpy.FeatureClassToFeatureClass_conversion(
        pnts, arcpy.env.scratchFolder, "WaterPoints")


def getPopNotServed(water_points_buff, pop_grid, urban_area=None):
    """Extracts the population unserved by water points from population grid"""

    # Get path to population data
    path = Toolbox.dict_population_sources[pop_grid]["Raster"]
    cell_size = Toolbox.dict_population_sources[pop_grid]["LowPS"]

    try:
        arcpy.AddMessage("Population Grid Cell Size: {}".format(cell_size))
    except:
        arcpy.AddError(
            "ERROR: Path to {} population data is incorrect".format(pop_grid))
        sys.exit(1)

    # arcpy.env.snapRaster = pop_grid
    # need a way to extract the correct item from mosaic dataset instead of using mosaic itself as snap raster

    # filter out urban areas where water points aren't necessary
    if urban_area:
        start = time.clock()
        polygon_served = arcpy.Merge_management(
            [water_points_buff, urban_area], r"in_memory\served_poly"
        )  # results are different now! compare to 2x con method
        arcpy.AddMessage(
            "Merge took: {:.2f} seconds".format(time.clock() - start))
    else:
        polygon_served = water_points_buff

    # arcpy.env.snapRaster = pop_grid
    start = time.clock()
    oid = [f.name for f in arcpy.Describe(polygon_served).fields][0]
    area_served = arcpy.PolygonToRaster_conversion(
        polygon_served, oid, r"in_memory\served", 'CELL_CENTER', 'NONE',
        cell_size)
    arcpy.AddMessage(
        "Rasterize took: {:.2f} seconds".format(time.clock() - start))
    # add a better error for when the extent is too big for memory
    # arcpy.env.snapRaster = area_served

    # Use Con tool to set population to 0 in raster cells that have access to water
    start = time.clock()
    area_not_served = arcpy.sa.IsNull(area_served)  #, r"in_memory\not_served")
    # Seems to fail on this con statement a lot: ExecuteError: ERROR 999999: Error executing function.
    arcpy.env.workspace = "in_memory"
    pop_not_served = arcpy.sa.Con(area_not_served, path, '0', 'Value > 0')
    # pop_not_served = arcpy.gp.Con_sa(area_not_served, path, r"in_memory\not_served1", '0', 'Value > 0')
    arcpy.AddMessage(
        "Conditional statement took: {:.2f} seconds".format(time.clock() -
                                                            start))
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
        zone = parameters[0].valueAsText
        num = parameters[1].valueAsText
        buff_dist = parameters[2].valueAsText
        pop_grid = get_all_image_sources().keys()[0]
        Toolbox.dict_population_sources = get_all_image_sources()

        # Query WPDx database
        mask, cc = setEnvironment(zone)
        query_response = queryWPDx(cc)
        pnts = getWaterPoints(query_response)

        start = time.clock()
        pnts_func = arcpy.MakeFeatureLayer_management(pnts, 'Functioning',
                                                      "status_id='yes'")
        arcpy.SelectLayerByLocation_management(pnts_func, 'within', mask)

        pnts_buff = arcpy.Buffer_analysis(pnts_func, r"in_memory\buffer",
                                          "{} Meters".format(buff_dist))
        arcpy.AddMessage("Selected {} points in your admin zone".format(
            int(arcpy.GetCount_management(pnts_buff).getOutput(0))))
        if mask == "Error":
            arcpy.env.extent = arcpy.Describe(pnts_buff).extent
        arcpy.AddMessage(
            "Buffer took: {:.2f} seconds".format(time.clock() - start))

        pop_not_served = getPopNotServed(pnts_buff, pop_grid, fc_area_urban)

        cell_size = float(
            Toolbox.dict_population_sources[pop_grid]["LowPS"]) * 111000

        cell_factor = int(round(float(buff_dist) / cell_size))
        neighborhood = arcpy.sa.NbrCircle(cell_factor, "CELL")

        FocalStat = arcpy.sa.FocalStatistics(pop_not_served, neighborhood,
                                             "SUM", "DATA")

        agg = arcpy.sa.Aggregate(FocalStat, cell_factor, 'MAXIMUM')
        #arcpy.env.mask = mask
        agg_pnts = arcpy.RasterToPoint_conversion(agg, r"in_memory\agg_pnt",
                                                  'Value')
        agg_pnts_lyr = arcpy.MakeFeatureLayer_management(agg_pnts)
        arcpy.SelectLayerByLocation_management(agg_pnts_lyr, 'within', mask)
        sort = arcpy.Sort_management(agg_pnts_lyr, r"in_memory\sort",
                                     'grid_code DESCENDING')
        top = arcpy.MakeFeatureLayer_management(sort, 'TOP',
                                                "OBJECTID<{}".format(int(num)+1))
        arcpy.AlterField_management(top, "grid_code", "Pop_Served",
                                    "Pop_Served")
        output = arcpy.CopyFeatures_management(
            top, join(arcpy.env.scratchGDB, "NewLocations")).getOutput(0)

        parameters[3].value = output
        parameters[4].value = self.outputCSV(output, zone)

        # should zones close to broken points count as good locations for a new installation?

    def outputCSV(self, fc, zone):
        """Creates output csv file"""
        arcpy.AddField_management(fc, 'Longitude', 'FLOAT')
        arcpy.AddField_management(fc, 'Latitude', 'FLOAT')
        arcpy.CalculateField_management(fc, 'Latitude', '!SHAPE!.firstPoint.Y',
                                        'PYTHON_9.3')
        arcpy.CalculateField_management(fc, 'Longitude',
                                        '!SHAPE!.firstPoint.X', 'PYTHON_9.3')

        fields = [field.name for field in arcpy.Describe(fc).fields]
        fields.remove('pointid')
        fields.remove('Shape')
        file_path = join(arcpy.env.scratchFolder,
                         "{}_NewLocations.csv".format(zone))
        with open(file_path, 'w') as out_csv:
            writer = csv.writer(out_csv)
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
        Param4.symbology = lyr_new_locations
        Param4.value = r"in_memory\NewLocations"

        return [Param0, Param1, Param2, Param4, Param5]

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
        with arcpy.da.SearchCursor(pnts_buff, 'wpdx_id',
                                   "status_id='no'") as cursor:
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
            point = arcpy.MakeFeatureLayer_management(
                pnts_buff, pnt, "wpdx_id='{}'".format(pnt))
            incr_pop = arcpy.gp.ZonalStatisticsAsTable_sa(
                point, 'wpdx_id', pop_grid, r"in_memory\pop{}".format(pnt_id),
                'DATA', 'SUM')
            with arcpy.da.SearchCursor(incr_pop, ['wpdx_id', 'SUM']) as cursor:
                for row in cursor:
                    pop_dict[row[0]] = row[1]
        #############################################################################

        arcpy.AddMessage(
            "Zonal Stats took: {:.2f} seconds".format(time.clock() - start))
        return pop_dict

    def sort_csv(self, in_path, out_path): 
        data = pd.read_csv(in_path)
        sortedlist = data.sort_values('Pop_Served', ascending=False)
        sortedlist.to_csv(out_path, index=False,
             columns=['Pop_Served','lat_deg','lon_deg', 'wpdx_id','country_id', 'country_name',
                      'adm1','adm2','management','subjective_quality','fecal_coliform_presence','fecal_coliform_value',
                      'created', 'install_year', 'installer','photo_lnk', 'report_date', 
                      'source', 'status_id', 'updated', 'water_source', 'water_tech','_notes','_scheme_ID'])
        return out_path

    def outputCSV(self, zone, points, pop_dict):
        """Creates output csv file"""
        keys = set()
        keys.add("Pop_Served")
        for line in points:
            keys.update(line.keys())
        ordered_keys = sorted(keys)
        file_path_temp = join(arcpy.env.scratchFolder,
                         "{}_RepairPriority_temp.csv".format(zone))
        file_path = join(arcpy.env.scratchFolder,
                         "{}_RepairPriority.csv".format(zone))
        with open(file_path_temp, 'wb') as out_csv:
            writer = csv.DictWriter(out_csv, ordered_keys)
            writer.writeheader()
            for line in points:

                if line['status_id'] == 'yes':
                    continue
                site_id = line['wpdx_id']
                try:
                    line['Pop_Served'] = pop_dict[site_id]
                except:
                    continue
                writer.writerow(line)
        out_csv = self.sort_csv(file_path_temp, file_path)
        return file_path

    def execute(self, parameters, messages):
        """The source code of the tool."""

        # Get Parameters
        zone = parameters[0].valueAsText
        buff_dist = parameters[1].valueAsText
        pop_grid = get_all_image_sources().keys()[0]
        Toolbox.dict_population_sources = get_all_image_sources()

        # Calculate incremental population that could be served by each broken water point
        mask, cc = setEnvironment(zone)
        query_response = queryWPDx(cc)
        pnts = getWaterPoints(query_response)
        pnts_lyr = arcpy.MakeFeatureLayer_management(pnts)
        arcpy.SelectLayerByLocation_management(pnts_lyr, 'within', mask)

        start = time.clock()
        pnts_buff = arcpy.Buffer_analysis(pnts_lyr, r"in_memory\buffer",
                                          "{} Meters".format(buff_dist))
        arcpy.AddMessage("Selected {} points in your admin zone".format(
            int(arcpy.GetCount_management(pnts_buff).getOutput(0))))
        if mask == "Error":
            arcpy.env.extent = arcpy.Describe(pnts_buff).extent
        arcpy.AddMessage(
            "Buffer took: {:.2f} seconds".format(time.clock() - start))
        pnts_buff_func = arcpy.MakeFeatureLayer_management(
            pnts_buff, 'Functioning', "status_id='yes'")
        pop_not_served = getPopNotServed(pnts_buff_func, pop_grid)

        # Add population served to water points as an attribute
        pop_dict = self.calcPriority(pnts_buff, pop_not_served)
        arcpy.AddField_management(pnts_lyr, "Pop_Served", "FLOAT")
        pnts_nonfunc = arcpy.MakeFeatureLayer_management(
            pnts_lyr, 'NonFunctioning', "status_id='no'")
        with arcpy.da.UpdateCursor(pnts_nonfunc,
                                   ['wpdx_id', 'Pop_Served']) as cursor:
            for row in cursor:
                try:
                    row[1] = pop_dict[row[0]]
                    cursor.updateRow(row)
                except KeyError:
                    pass

        output = arcpy.CopyFeatures_management(
            pnts_nonfunc, join(arcpy.env.scratchGDB,
                               "RepairPriority")).getOutput(0)

        parameters[2].value = output
        parameters[3].value = self.outputCSV(
            zone, query_response, pop_dict)  #this is not filtered by the mask!

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
        Param3.symbology = lyr_repair_priority_esri
        Param3.value = r"in_memory\RepairPriority"
        return [Param0, Param1, Param3, Param4]

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
        pop_by_region = arcpy.gp.ZonalStatisticsAsTable_sa(
            admin_zones, 'Name', unserved_population, r"in_memory\pop", '',
            'SUM')
        with arcpy.da.SearchCursor(pop_by_region, ['Name', 'SUM']) as cursor:
            for row in cursor:
                pop_dict[row[0]] = row[1]
        arcpy.AddMessage(
            "Zonal stats took: {:.2f} seconds".format(time.clock() - start))
        return pop_dict

    def outputCSV(self, Country, fc):
        """Creates output csv file"""
        fields = ['Name','Type','Type_ENG','Admin1','Country','CC','Rural_Pop_Esri','Pop_Unserved','Percent_Unserved']
        fields1 = ['Name','Type','Type_ENG','Admin1','Country','CC','Total_Rural_Pop','Pop_Unserved','Percent_Unserved']
        file_path = join(arcpy.env.scratchFolder,
                         "{}_ServiceOverview.csv".format(Country))
        with open(file_path, 'wb') as out_csv:
            writer = csv.writer(out_csv)
            writer.writerow(fields1)
            with arcpy.da.SearchCursor(fc, fields) as rows:
                for row in rows:
                    writer.writerow(row)
        return file_path

    def execute(self, parameters, messages):
        """Calculates percentage of population unserved in each administrative area."""

        # Get Paramters
        country = parameters[0].valueAsText
        buff_dist = parameters[1].valueAsText
        pop_grid = get_all_image_sources().keys()[0]
        Toolbox.dict_population_sources = get_all_image_sources()

        # Query WPDx database
        mask, cc = setEnvironment(country)
        query_response = queryWPDx(cc)
        pnts = getWaterPoints(query_response)

        # Calculate percentage of population unserved in each administrative area
        start = time.clock()
        pnts_func = arcpy.MakeFeatureLayer_management(pnts, 'Functioning',
                                                      "status_id='yes'")
        arcpy.SelectLayerByLocation_management(pnts_func, 'within', mask)
        pnts_buff = arcpy.Buffer_analysis(pnts_func, r"in_memory\buffer",
                                          "{} Meters".format(buff_dist))
        # would buffer be faster in different coordinate system?
        arcpy.AddMessage(
            "Buffer took: {:.2f} seconds".format(time.clock() - start))

        pop_not_served = getPopNotServed(pnts_buff, pop_grid, fc_area_urban)
        pop_dict = self.calcUnserved(mask, pop_not_served)
        output = arcpy.CopyFeatures_management(
            mask, join(arcpy.env.scratchGDB, "ServiceOverview")).getOutput(0)

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
        arcpy.CalculateField_management(
            output, 'Percent_Unserved',
            'round(!Pop_Unserved!/!Rural_Pop_{}!,2)'.format("Esri"),
            'Python')
        arcpy.CalculateField_management(
            output, 'Percent_Unserved', 'max(0, !Percent_Unserved!)', 'PYTHON_9.3')

        parameters[2].value = output
        parameters[3].value = self.outputCSV(country, output)

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

        Param0.value = 'Tanzania'
        Param1.value = '1000'
        Param3.symbology = lyr_overview
        Param3.value = "in_memory\ServiceOverview"
        return [Param0, Param1, Param3, Param4]

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
        zone = parameters[0].valueAsText
        buff_dist = parameters[1].valueAsText
        out_path = parameters[2].value
        pop_grid = get_all_image_sources().keys()[0]
        Toolbox.dict_population_sources = get_all_image_sources()

        # Query WPDx database
        mask, cc = setEnvironment(zone)
        query_response = queryWPDx(cc)
        pnts = getWaterPoints(query_response)

        start = time.clock()
        pnts_func = arcpy.MakeFeatureLayer_management(pnts, 'Functioning',
                                                      "status_id='yes'")
        arcpy.SelectLayerByLocation_management(pnts_func, 'within', mask)
        pnts_buff = arcpy.Buffer_analysis(pnts_func, r"in_memory\buffer",
                                          "{} Meters".format(buff_dist))
        if mask == "Error":
            arcpy.env.extent = arcpy.Describe(pnts_buff).extent
        arcpy.AddMessage(
            "Buffer took: {:.2f} seconds".format(time.clock() - start))

        pop_not_served = getPopNotServed(pnts_buff, pop_grid, fc_area_urban)
        masked = arcpy.sa.ExtractByMask(pop_not_served, mask)
        output = arcpy.CopyRaster_management(masked, out_path)
        parameters[2] = output  # arcpy.MakeRasterLayer_management(masked, "out_raster")


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
            displayName='Output Features',
            name='out_feat',
            datatype='DERasterDataset',
            parameterType='Derived',
            direction='Output')

        Param0.value = 'Arusha'
        Param1.value = '1000'
        Param2.value = r"C:\ArcGIS_Files\BlueRaster\WPDx-Toolset-master\PopNotServed_output.tif"
        Param2.symbology = lyr_popnotserved
        # return [Param0, Param1, Param2, Param3]
        return [Param0, Param1, Param2]

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

        country = parameters[0].valueAsText
        mask, cc = setEnvironment(country)

        # arcpy.env.snapRaster = pop_grid
        cell_size = '0.0008333'
        start = time.clock()
        area_served = arcpy.PolygonToRaster_conversion(
            fc_area_urban, 'RANK', join(arcpy.env.scratchGDB, 'served'),
            'CELL_CENTER', 'NONE', cell_size)

        area_not_served = arcpy.gp.IsNull(area_served)
        arcpy.AddMessage(
            "Rasterize took: {:.2f} seconds".format(time.clock() - start))
        start = time.clock()

        Toolbox.dict_population_sources = get_all_image_sources()

        for name in Toolbox.dict_population_sources:
            #try:
            pop_grid = Toolbox.dict_population_sources[name]["Raster"]

            pop_not_served = arcpy.sa.Con(area_not_served, pop_grid, '0',
                                          'Value>0')
            arcpy.AddMessage("Conditional statement took: {:.2f} seconds".
                             format(time.clock() - start))

            start = time.clock()
            pop_by_region = arcpy.gp.ZonalStatisticsAsTable_sa(
                fc_adm_zones, 'Name', pop_not_served,
                r"in_memory\pop{}".format(name), '', 'SUM')
            arcpy.AddMessage(
                "Zonal Stats took: {:.2f} seconds".format(time.clock() -
                                                          start))
            pop_dict = dict()

            # populate pop_dict
            with arcpy.da.SearchCursor(pop_by_region,
                                       ['Name', 'SUM']) as cursor:
                for row in cursor:
                    pop_dict[row[0]] = row[1]

            # join pop_dict to admin areas
            with arcpy.da.UpdateCursor(fc_adm_zones,
                                       ['Name', 'Rural_Pop_{}'.format("Esri")],
                                       "{} = '{}'".format('CC', cc)) as cursor:
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


class UrbanThreshold(object):
    def __init__(self):
        """Delineates urban areas based on a populationd density threshold"""
        self.label = 'Urban Threshold'
        self.description = 'Delineates urban areas based on a populationd' + \
                           'density threshold.'
        self.category = "Utilities"

    def execute(self, parameters, messages):
        """Calculates rural population in each administrative area."""

        country = parameters[0].valueAsText
        thresh = parameters[1].valueAsText
        Toolbox.dict_population_sources = get_all_image_sources()
        pop_grid = get_all_image_sources().keys()[0]
        path = Toolbox.dict_population_sources[pop_grid]["Raster"]

        mask, cc = setEnvironment(country)

        start = time.clock()
        rc = arcpy.sa.Reclassify(
            path, 'VALUE',
            arcpy.sa.RemapRange([[-999, thresh, "NODATA"],
                                 [thresh, 99999999, 1]]), 'DATA')
        arcpy.AddMessage(
            "Reclassify took: {:.2f} seconds".format(time.clock() - start))
        start = time.clock()
        output = arcpy.RasterToPolygon_conversion(
            rc, join(arcpy.env.scratchGDB, "out"), 'SIMPLIFY', 'Value', 'SINGLE_OUTER_PART')
        arcpy.AddMessage(
            "Polygonize took: {:.2f} seconds".format(time.clock() - start))
        output_lyr = arcpy.MakeFeatureLayer_management(output)
        arcpy.SelectLayerByLocation_management(output_lyr, 'intersect', mask)

        out_final = arcpy.CopyFeatures_management(
            output_lyr, join(arcpy.env.scratchGDB, "UrbanAreas")).getOutput(0)
        parameters[2] = out_final

    def getParameterInfo(self):
        """Define parameter definitions"""
        Param0 = arcpy.Parameter(
            displayName='Country',
            name='country',
            datatype='GPString',
            parameterType='Required',
            direction='Input')
        Param0.value = 'Swaziland'

        Param1 = arcpy.Parameter(
            displayName='Population Density Threshold',
            name='thresh',
            datatype='GPString',
            parameterType='Required',
            direction='Input')
        Param1.value = '2000'

        Param2 = arcpy.Parameter(
            displayName='Output Features',
            name='out_feat',
            datatype='DEFeatureClass',
            parameterType='Derived',
            direction='Output')


        return [Param0, Param1, Param2]

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