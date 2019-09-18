# ConcertoSection, Sandbox and FieldUpdater classes
# See concerto_v4.py for changelog

import datetime
import itertools
import os
import time

import arcpy
from arcpy import env

from concerto_v4 import MAIN_PATH, MAIN_SHAPE_PATH, CPAD_PATH, SANDBOX, message

class ConcertoSection(object):
    """ Instances represent one of the 4 sections of the script, namely:
        Acquisitions & Disposals, Cases, Leases, and Sites.
    """
    def __init__(self, cpad_view_name, shape_key_field, cpad_key_field,
                 shapefile_folder, feature_class_prefix):
        self.cpad_view_name = cpad_view_name
        self.shape_key_field = shape_key_field
        self.cpad_key_field = cpad_key_field
        self.shapefile_folder = shapefile_folder
        self.feature_class_prefix = feature_class_prefix
        self.shape_fc = "{}_Shape".format(self.feature_class_prefix)
        self.poly_fc = "{}_Poly".format(self.feature_class_prefix)
        self.cpad_fc = "{}_CPAD".format(self.feature_class_prefix)
        self.layer_fc = "{}_Layer".format(self.feature_class_prefix)
        self.live_fc = "{}_Live".format(self.feature_class_prefix)
        self.shape_input = os.path.join(MAIN_SHAPE_PATH, self.shapefile_folder)
        self.shape_output = os.path.join(MAIN_PATH, self.shape_fc)
        self.cpad_view_path = os.path.join(CPAD_PATH, self.cpad_view_name)
        self.sort_shape = []
    
    def __repr__(self):
        """ Prints the command that creates the class """
        return "ConcertoSection({})".format(self.feature_class_prefix)
    
    def wrapper(self):
        """ Tidies, verifies, processes, imports, joins and copies, processes and
        renames, using our methods. This controls the general workflow.
        """
        message("**************************************************")
        message("Starting {}".format(self.feature_class_prefix).center(50))
        message("**************************************************")
        self.tidy_list = self.tidy_start()
        self.verified_shapes = self.verify_shapes(self.shape_input, self.shape_key_field)
        if self.verified_shapes:
            try:
                self.process_new_shapes(self.verified_shapes)
                self.import_CPAD_table()
                self.layer_join_and_copy()
                self.process_updates()
                self.finally_rename_shapes(self.verified_shapes)
            except Exception as e:
                message("~ Unable to update {}. {}".format(self.live_fc, str(e)))
        else:
            message("**************************************************")
            message("No records found for processing".center(50))
            message("**************************************************")
        self.retire_old_files()
        # These 2 lines may seem out of place, but they're needed for the
        # FieldUpdater to run later.
        if not arcpy.Exists(os.path.join(MAIN_PATH, self.cpad_fc)):
            # Remove this next line if Cases ever have fields to update and
            # unindent the call to import_CPAD_table()
            if not self.feature_class_prefix == "Cases":
                self.import_CPAD_table()
        message("**************************************************")
        message("{} completed".format(self.feature_class_prefix).center(50))
        message("**************************************************")
    
    # Some functions that tidy up the workspace, delete old files etc.

    def tidy_start(self):
        """ creates tidy_list, calls tidy_delete_old_files."""
        self.tidy_list = [self.shape_fc, self.poly_fc, self.cpad_fc]
        self.tidy_delete_old_files(self.tidy_list)
        return self.tidy_list
    
    def tidy_delete_old_files(self, tidy_list):
        """ Tries to delete each item in tidy_list. """
        for item in tidy_list:
            arcpy.RefreshCatalog(MAIN_PATH)
            item_path = os.path.join(MAIN_PATH, item)
            if arcpy.Exists(item_path):
                try:
                    arcpy.Delete_management(item_path)
                    message("{} deleted".format(item))
                except Exception as e:
                    message("~ Unable to delete {}. {}".format(item, str(e)))
                    # raise
    
    def tidy_delete_old_fcs_and_tables(self):
        """ Makes list of all fcs and tables in env.workspace with names ending
        '_old' or '_temp', tries to delete them.

        Old and Temp fields will be remnants of previous script versions so
        this is a legacy function, but there's no harm in keeping it around
        unless performance suddenly becomes a major issue.
        """
        for item in [name for name in arcpy.ListFeatureClasses() +
                     arcpy.ListFields() if name.endswith("_old") or
                     name.endswith("_temp")]:
            item_path = os.path.join(MAIN_PATH, item)
            try:
                arcpy.Delete_management(item_path)
                message("{} deleted".format(item))
            except Exception as e:
                message("~ Unable to delete {}. {}".format(item, str(e)))
                # raise
    
    def tidy_end(self):
        """ Calls tidy_delete_old_files and tidy_delete_old_fcs_and_tables.

        Not called until after FieldUpdater has done its work, as that
        needs to use Section_CPAD.
        """
        message("Cleaning up old files")
        self.tidy_delete_old_files(self.tidy_list)
        self.tidy_delete_old_fcs_and_tables()
    
    # End of tidying functions

    def has_lock_file(self, shapefile):
        """ Determines whether a shapefile has a corresponding lock file in
        the same folder. i.e. whether it's still being worked on.
        """
        shapename = shapefile[:-4]
        check_list = [name for name in os.listdir(self.shape_input) if
                      name.startswith(shapename) and name.endswith("lock")]
        if check_list:
            message("{} is locked. Skipping verification".format(shapename))
            return True
    
    def create_list_of_shapes_to_verify(self):
        """ Returns a list of items in shapefile_folder that fit the pattern
        'name.shp' and are not currently being worked on.
        """
        return [item for item in os.listdir(self.shape_input) if
                item.endswith(".shp") and not item.startswith("__") and
                not self.has_lock_file(item)]
    
    def verify_key_field(self, item):
        """ Checks the field 'shape_key_field' within item. Strips whitespace.
        If the field length is greater than 1, verifies it. Otherwise marks
        as invalid. Returns number of valid and invalid rows.
        """
        valid_rows = 0
        invalid_rows = 0
        with arcpy.da.UpdateCursor(os.path.join(self.shape_input, item),
                                   self.shape_key_field) as ucursor:
            for row in ucursor:
                row[0].strip()
                ucursor.updateRow(row)
                if len(row[0]) > 1:
                    valid_rows += 1
                else:
                    # Not comfortable deleting data here. See documentation
                    # for discussion.
##                    ucursor.deleteRow()
                    invalid_rows += 1
                    message("@ '{}' is niot a valid {}. Removed row".format(
                            row[0], self.shape_key_field))
        return valid_rows, invalid_rows
    
    def verify_shapes(self, shapefile_folder, shape_key_field):
        """ Verifies input files, calls del_duds() to delete failures.

        To verify it checks that it contains a field 'shape_key_field' and calls
        verify_key_field() to check that it's populated.
        """
        dud_list = []
        good_list = []
        input_list = self.create_list_of_shapes_to_verify()
        for item in input_list:
            try:
                field_list = [field.name for field in arcpy.ListFields(
                    os.path.join(self.shape_input, item))]
                if not field_list:
                    dud_list.append(item)
                    message(("~ {} has no fields. "
                             "Added to dud list").format(item))
                elif shape_key_field not in field_list:
                    dud_list.append(item)
                    message("~ Field '{}' not found. Added to dud list".format(
                        self.shape_key_field))
                else:
                    valid_rows, invalid_rows = self.verify_key_field(item)
                    if valid_rows <= 0:
                        dud_list.append(item)
                        message(("~ '{}' has no valid rows. "
                                "Added to dud list").format(item))
                    else:
                        good_list.append(item)
                        if invalid_rows > 0:
                            message(("Verified '{}' with {} valid and {} "
                                     "invalid shape(s)").format(
                                         item ,str(valid_rows),
                                         str(invalid_rows)))
                        else:
                            message(("Completed verifying '{}' "
                                     "with {} valid shape(s)").format(
                                     item, str(valid_rows)))
            except RuntimeError as e:
                message("~ Unable to attempt verification on {}. {}".format(
                    item, str(e)))
                # No need to raise here, as the rest of the shapes can still
                # be processed.
        if dud_list:
            self.del_duds(dud_list)
        return good_list
    
    def del_duds(self, dud_list):
        """ Deletes shapefiles that have failed verification.

        Called from within verify_shapes() to remove invalid files.
        dud_list is only files that COMPLETELY fail verification. If some
        polygons in a shapefile fail and others pass, verify_shapes() handles
        it by removing the malformed polys and verifying the rest.
        """
        for item in dud_list:
            message("@ {} was malformed. Please investigate".format(item))
            # Not comfortable deleting - it's a bit final. It's also rare enough
            # that manual investigation isn't a huge cost. Hence this bit being
            # commented out.
##            try:
##                arcpy.Delete_management(os.path.join(self.shapefile_folder, item))
##                message("Malformed file {} deleted".format(item))
##            except Exception as e:
##                message("~ Unable to delete malformed item {}. {}".format(item, str(e)))
##                raise
    
    def print_locked_files(self):
        """ Prints all files in the .gdb folder with a name ending ".lock"
            Added in 4.2 for troubleshooting purposes.
        """
        lockfiles = [item for item in os.listdir(MAIN_PATH)
                     if item.endswith(".lock")]
        message("{} locked files in {}".format(len(lockfiles), MAIN_PATH))
        for locked_file in lockfiles:
            message("~ Locked file: {}".format(locked_file))

    def process_new_shapes(self, verified_shapes):
        """ Concatenates all the shapes from verified_shapes into a single
        feature class, that can then be joined to and added to the live
        geodatabase as a whole.
        """
        message("Beginning processing of new shape data")
        time.sleep(60)
        # self.shape_output shouldn't exist thanks to tidy_start, but this will
        # handle it if it does
        if arcpy.Exists(self.shape_output):
            for item in verified_shapes:
                try:
                    arcpy.Append_management(os.path.join(
                        self.shape_input, item), self.shape_output, "NO_TEST")
                    message("{} appended to {}".format(item, shape_fc))
                except Exception as e:
                    message(("~ Unable to append {}. Also, why am I appending "
                             "rather than merging? Something's up. {}").format(
                                 item, str(e)))
                    raise
    # This is the block that will typically run
        else:
            try:
                arcpy.Merge_management(
                    [os.path.join(self.shape_input, item) for
                    item in verified_shapes], self.shape_output)
                message("Successfully merged shapes into {}".format(
                    self.shape_fc))
            except Exception as e:
                self.print_locked_files() # added in 4.2
                message("~ Unable to merge shapes into {}. {}".format(
                    self.shape_fc, str(e)))
                raise
        message("Processing of new shape data complete")
    
    def import_CPAD_table(self):
        """ Imports a table from CPAD into Concerto
        """
        # Requires the machine running the script to have SQL Server client installed
        message("Beginning import of CPAD table")
        full_path = os.path.join(MAIN_PATH, self.cpad_fc)
        try:
            arcpy.TableToTable_conversion(self.cpad_view_path,
                                          MAIN_PATH, self.cpad_fc)
            # Potential for test here - did AddJoin actually add anything,
            # or was CPAD empty?
            message("{} created".format(self.cpad_fc))
        except Exception as e:
            message("~ Unable to create {}. {}".format(self.cpad_fc, str(e)))
            raise
    
    def clean_temp_fc(self):
        """ If 'Section_Poly_temp' exists, delete it.

        It should have been deleted by tidy_end() ast time the script ran, 
        but this will save a crash if it wasn't.
        """
        temp_fc = "{}_temp".format(self.poly_fc)
        if arcpy.Exists(temp_fc):
            try:
                arcpy.Delete_management(temp_fc)
            except Exception as e:
                message("~ Unable to delete {}. Please Check.\n{}".format(
                    temp_fc, str(e)))
                raise
    
    def create_new_layer(self):
        """ Converts shape_fc (a feature class) to layer_fc (a feature layer)
        as the join only works on layers.
        """
        message("Attempting to create layer")
        try:
            arcpy.MakeFeatureLayer_management(self.shape_fc, self.layer_fc)
            message("Successfully created {}".format(
                self.layer_fc))
        except Exception as e:
            message("~ Unable to make Feature Layer {}. {}".format(
                self.layer_fc, str(e)))
            raise
    
    def perform_join(self):
        """ Joins cpad_fc to layer_fc. """
        message("Beginning Join")
        try:
            arcpy.AddJoin_management(self.layer_fc, self.shape_key_field,
                                     self.cpad_fc, self.cpad_key_field,
                                     "KEEP_COMMON") # remove this if we're ok
                                     # with lots of null values in output table
            message("Successfully joined {} to {}".format(self.cpad_fc, 
                                                          self.layer_fc))
        except Exception as e:
            message("~ Failed to join {} to {}. {}".format(
                self.cpad_fc, self.layer_fc, str(e)))
            raise
    
    def create_final_fc(self):
        """ Creates poly_fc from layer_fc, ready to merge into live_fc.

        The merge has to be done on a feature class, so we need to convert
        it back from a feature layer.
        """
        message("Creating output Feature Class")
        try:
            arcpy.CopyFeatures_management(
                self.layer_fc, os.path.join(MAIN_PATH, self.poly_fc))
            message("{} created successfully".format(self.poly_fc))
        except Exception as e:
            message("~ Unable to create {}. {}".format(self.poly_fc, str(e)))
            raise
    
    def layer_join_and_copy(self):
        """ Calls subsidiary functions to make a layer, join data to it and 
        return the final Feature Class.
        """
        self.clean_temp_fc()
        message("Beginning Layer join & copy")
        self.create_new_layer()
        self.perform_join()
        self.create_final_fc()
    
    def process_updates(self):
        """ Updates the live database table with the new shapes.

        First checks whether there are any updates to process.
        Then deletes existing values for any shapes that already have old data.
        Finally appends new shapes to live_fc.

        The other way to organise this would be without a cursor: to append
        first then delete duplicates. That requires keeping track of the most
        recent version of each record though, creating a whole other problem set.
        Let's stick with this method for as long as it's practicable.
        """
        message("Processing Updates")
        number_to_process = int(arcpy.GetCount_management(
            self.poly_fc).getOutput(0))
        if not arcpy.Exists(self.poly_fc) or number_to_process < 1:
            message("No updates to process")
            return
        replacement_records = 0
        new_records = 0
        check_list = self.create_master_list()
        poly_names = [field.name for field in arcpy.ListFields(self,poly_fc)]
        live_names = [field.name for field in arcpy.ListFields(self.live_fc)]
        polyindex = self.get_index()
        with arcpy.da.SearchCursor(self.poly_fc, poly_names) as scursor:
            for row in scursor:
                if row[polyindex] in check_list:
                    self.delete_record(row[polyindex])
                    replacement_records += 1
                else:
                    new_records += 1
        try:
            arcpy.Append_management(self.poly_fc, self.live_fc, "NO_TEST")
            message("{} shapes added to live. {} new and {} replacements".format(
                number_to_process, new_records, replacement_records))
        except Exception as e:
            message("~ Unable to add new shapes to live. {}".format(str(e)))
            raise
    
    def create_master_list(self):
        """ Creates a list of items in live_fc. Called by process_updates() """
        with arcpy.da.SearchCursor(self.live_fc, self.shape_key_field) as scursor:
            # removed .encode from row [0] as it works without
            master_list = [row[0] for row in scursor]
        return master_list
    
    def get_index(self):
        """ Returns the index location of self.shape_key_field

        Called by process_updates()
        """
        try:
            return [fld.name for fld in arcpy.ListFields(
                self.poly_fc)].index(self.shape_key_field)
        except ValueError as e:
            message("~ Unable to find {} in {}. \n{}".format(
                self.shape_key_field, self.poly_fc, str(e)))
            return None
    
    def delete_record(self, del_value):
        """ Deletes the specified record from live_fc Called by process_updates()
        """
        with arcpy.da.UpdateCursor(self.live_fc, self.shape_key_field) as ucursor:
            for row in ucursor:
                if row[0] == del_value:
                    try:
                        ucursor.deleteRow()
                        message("Old value of {} deleted from {}".format(
                            del_value, self.live_fc))
                    except Exception as e:
                        message(("~ Unable to delete old "
                        "value of {} from {}. \n{}").format(
                            del_value, self.live_fc, str(e)))
                        #raise
    
    def finally_rename_shapes(self, good_list):
        """ Renames shapes in good_list by calling rename_shape() on them.

        It's called 'finally' to make it clear it should only happen after
        the shape has been processed.
        """
        for item in good_list:
            try:
                self.rename_shape(item)
            except Exception as e:
                message("~ Unable to rename {}. Please check".format(item))
                # raise
                # see rename_shape() for the reason it's commented out
        
    def rename_shape(self, item):
        """ Renames item with prefix '__' to prevent reuse.

        Called by finally_rename_shapes only AFTER processing the shape.
        """
        # These conditions are checked in create_list_of_shapes_to_verify(),
        # hence lack of proper test here.
        assert item.endswith(".shp") and not item.startswith("__")
        new_name = "__{}".format(item)
        item_path = os.path.join(MAIN_SHAPE_PATH, self.shapefile_folder, item)
        new_path = os.path.join(MAIN_SHAPE_PATH, sef.shapefile_folder, new_name)
        try:
            arcpy.Rename_management(item_path, new_path))
            message("{} renamed to {}".format(item, new_name))
        except Exception as e:
            message("~ Unable to rename {}. Please check".format(item))
            # raise
            # commented out as we haven't implemented logic to handle locked
            # files (or any other reason renaming might have failed). For now
            # it's just marked as an error and can be investigated manuallly.
    
    def retire_old_files(self):
        """ Deletes files over 30 days old.

        It first checks they've been processed, and are in the format
        '__name.shp'
        """
        today = datetime.date.today()
        for item in os.listdir(self.shape_input):
            if item.startswith("__") and item.endswith(".shp"):
                item_path = os.path.join(self.shape_input, item)
                last_modified = os.path.getmtime(item_path)
                time_passed = datetime.date.fromtimestamp(last_modified)
                age = today - time_passed
                if age.days >= 30:
                    try:
                        arcpy.Delete_management(item_path)
                        message("{} deleted as {} days old".format(
                            item, str(age.days)))
                    except Exception as e:
                        message("~ Unable to delete {}. {} days old.\n{}".format(
                            item, str(age.days), str(e)))
                        # raise

###############################################################################

# This class is not currently used (call is commented out in main file).
# This is because network drive caching is interfering with ArcMap trying to 
# write to those drives.

class Sandbox(object):
    """ Update and verify the 'sandbox' geodatabase
    """
    def __init__(self):
        self.fc_list = ["Disposals_Live", "Cases_Live", "Leases_Live",
                        "Sites_Live", "ADREC", "POREC", "TLREC"]
    
    def update(self):
        """ Updates the second, 'sandbox' geodatbase that users can mess with
        """
        message("**************************************************")
        message("Updating Sandbox Geodatabase".center(50))
        message("**************************************************")
        env.workspace = SANDBOX
        old_fcs = [item for item in arcpy.ListFeatureClasses() if
                   item.endswith("_old") or itm.endswith("_new")]
        for item in old_fcs:
            try:
                arcpy.Delete_management(os.path.join(SANDBOX, item))
            except Exception as e:
                message("~ Unable to delete {}. Please check.\n{}".format(
                        item, str(e)))
                # raise
        for fc in self.fc_list:
            concerto_path = 