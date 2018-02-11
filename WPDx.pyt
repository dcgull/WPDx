import arcpy
from os.path import join
from sodapy import Socrata
import csv
import tempfile
#import cStringIO

# make sure to install these packages before running:
# pip install sodapy
#useful doc is here:
#https://dev.socrata.com/foundry/data.waterpointdata.org/gihr-buz6
#https://github.com/xmunoz/sodapy#getdataset_identifier-content_typejson-kwargs


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "WPDx Toolset"
        self.alias = "WPDx Toolbox"

        # List of tool classes associated with this toolbox
        self.tools = [RepairPriority]


class RepairPriority(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
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
        #Param2 should be a drop-down menu with aliases
        Param2.value = r'D:\GETF\Population Data\A4_NewPop150\TZ_0_Pop_150.tif'
        #Param2.value = r'D:\GETF\Population Data\worldpop\TZA_popmap15adj_v2b.tif'
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

    def execute(self, parameters, messages):
        """The source code of the tool."""

        #Get Parameters
        zone = parameters[0].valueAsText
        arcpy.AddMessage(zone)
        buff_dist = parameters[1].valueAsText
        pop_grid = parameters[2].value
        out_path = parameters[3].value
        cell_size = arcpy.Describe(pop_grid).meanCellWidth
        client = Socrata("data.waterpointdata.org", None)
        scratch = tempfile.mkdtemp()


        # First 2000 results, remove limit and get login if neccessary
        results = client.get("gihr-buz6", adm1 = zone, limit=2000, content_type='csv')

        with open (join(scratch, "{}_temp.csv".format(zone)), 'wb') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter='\t')
            for line in results:
                spamwriter.writerow(line)

        #Main
        #pnts = arcpy.JSONToFeatures_conversion(results, r"in_memory\in_points")    It might be possible avoid writing this csv file or to use JSON instead
        #pnts = arcpy.MakeXYEventLayer_management(cString.StringIO('\n'.join(['\t'.join(l) for l in results])), 'lon_deg', 'lat_deg', 'Temp_Layer', spatial_reference = arcpy.SpatialReference(4326))
        pnts = arcpy.MakeXYEventLayer_management(join(scratch, "{}_temp.csv".format(zone)), 'lon_deg', 'lat_deg', 'Temp_Layer', spatial_reference = arcpy.SpatialReference(4326))
        pnts1 = arcpy.CopyFeatures_management(pnts, r"in_memory\pnts")   #good opportunity to leave attributes out
        buffer = arcpy.Buffer_analysis(pnts1, r"in_memory\buffer", buff_dist)

        #Take only the functioning water points and rasterize them
        pnts_func = arcpy.MakeFeatureLayer_management(buffer, 'Functioning', "status_id='yes'")
        served = arcpy.PolygonToRaster_conversion(pnts_func, 'status_id', r"in_memory\served", 'CELL_CENTER', 'NONE', cell_size)

        #Use Con tool to set population to 0 in raster cells that have access to water
        arcpy.env.extent = arcpy.Describe(buffer).extent
        not_served = arcpy.gp.IsNull_sa(served, r"in_memory\nserved")
        pop_not_served = arcpy.gp.Con_sa(not_served, pop_grid, r"in_memory\popserved", '0', 'Value>0')

        #Calculate incremental population served by each water point
        pnts_nonfunc = arcpy.MakeFeatureLayer_management(pnts1, 'NonFunctioning', "status_id='no'")
        incr_pop = arcpy.gp.ZonalStatisticsAsTable_sa(pnts_nonfunc, 'wpdx_id', pop_not_served, r"in_memory\incr_pop", 'DATA', 'SUM')
        pop_by_pnt=dict()
        pop_by_pnt['wpdx_id'] = 'Pop_Served_Incr'
        with arcpy.da.SearchCursor(incr_pop, ['wpdx_id', 'SUM' ]) as cursor:
            for row in cursor:
                pop_by_pnt[row[0]] = row[1]
        arcpy.AddField_management(pnts_nonfunc, "Pop_Served_Incr", "FLOAT")
        with arcpy.da.UpdateCursor(pnts_nonfunc, ['wpdx_id', 'Pop_Served_Incr']) as cursor:
            for row in cursor:
                try:
                    row[1] = pop_by_pnt[row[0]]
                    cursor.updateRow(row)
                except KeyError:
                    pass

        output = arcpy.Project_management(pnts_nonfunc, out_path, arcpy.SpatialReference(3857))
        parameters[3] = output
        return output

        with open (join(scratch, "{}.csv".format(zone)), 'wb') as out_csv:
            spamwriter = csv.writer(out_csv, delimiter='\t')
            for line in results:
                site_id = line[36]
                try:
                    line.append(pop_by_pnt[site_id])
                except:
                    line.append(0)
                spamwriter.writerow(line)

        parameters[4] = out_csv

#Getting only 5 gages instead of 31
#out_csv isn't working
#add parameter to exclude points with insufficient quantity
#zonal stats doesn't handle overlapping polygons, have to iterate