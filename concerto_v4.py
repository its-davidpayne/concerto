# Concerto Script
# Version 4 - rework - David Payne - Corporate GIS - begin Sept 18
# This version (4.2) 23/07/2019
# Original by <redacted> - Corporate GIS 21/11/2014

import os
import shutil
import time

import arcpy
from arcpy import env

# contains the ConcertoSection and FieldUpdater classes
import concerto_classes


# 4.2 changes 23/07/2019:
# - Added new method to ConcertoSection to list locked files in case of error.
#   This should help track down an error we've been seeing a lot.
#
# 4.1 changes:
# - Removed Sandbox class in concerto_classes and all the logic for creating and
#   verifying the sandbox geodatabase. Replaced this with a simple shutil solution
#   that deletes and recreates the sandbox every day.
#
# 4.0 changes:
# - Significant rework. Created the class ConcertoSection to contain most of
#   the functions. Moved that to its' own .py file (concerto_classes.py).
# - Moved Sandbox creation and checking functions to that same .py, in a class
#   called Sandbox.
# - Removed landlord_update() but used the same basic logic to create the class
#   FieldUpdater (again, in the other .py) that will compare any field between
#   Concerto and CPAD and update Concerto where necessary.

# Global Constants.
# The GDB we write to:
MAIN_PATH = "\\\\partially_redacted\\Concerto\\Data\\Concerto.gdb"
# Where we get the input shapefiles from
MAIN_SHAPE_PATH = "\\\\partially_redacted\\GIS\\CPAD"
# The source of the data we combine with the new shapefiles
CPAD_PATH = "\\\\partially_redacted\\Connection to Concerto.sde"
# Where the script logs its actions
LOG_FILE = "\\\\partially_redacted\\Concerto\\Scripts\\logfile.txt"
# A second, non-critical GDB we write to for users to 'play' with
SANDBOX = "\\\\partially_redacted\\GIS\\CPAD\\RecordsManagement.gdb"

env.workspace = MAIN_PATH
env.qualifiedFieldNames = False
env.overwriteOutput = True


def message(msg):
    """ Appends msg to LOG_FILE, with timestamp."""
    # Let's print to console too. Can remove if requested.
    print ("{} - {}\n".format(time.asctime(), msg))
    with open(LOG_FILE, 'a') as log:
        log.write("{} - {}\n".format(time.asctime(), msg))


if __name__ == '__main__':
    start = time.time()
    message("##################################################")
    message("**************************************************")#
    message("**************************************************")
    message("Concerto Browser Data Import Script".center(50))
    message("version 4.2, 23/07/2019".center(50))
    message("David Payne - Corporate GIS".center(50))
    message("**************************************************")
    message("**************************************************")
    AcqDisp = concerto_classes.ConcertoSection(
        "Concerto.dbo.V_GIS_ACQUISITION_DISPOSAL", "REFVAL", "EstatesRef",
        "AcqDisp", "Disposals")
    AcqDisp.wrapper()
    Cases = concerto_classes.ConcertoSection(
        "Concerto.dbo.V_CM_to_GIS", "REFVAL", "Job_Number", "Case", "Cases")
    Cases.wrapper()
    Leases = concerto_classes.ConcertoSection(
        "Concerto.dbo.V_GIS_LEASE", "REFVAL", "EstatesRef", "Leases", "Leases")
    Leases.wrapper()
    Sites = concerto_classes.ConcertoSection(
        "Concerto.dbo.V_GIS_SITE", "UPRN", "SITE_UPRN", "Sites", "Sites")
    Sites.wrapper()
    time.sleep(10)
    # FieldUpdater part
    message("**************************************************")
    message("**************************************************")
    message("Checking fields for updates".center(50))
    message("**************************************************")
    AcqDispUpdater = concerto_classes.FieldUpdater("Disposals")
    AcqDispUpdater.update()
    AcqDisp.tidy_end()
    message("**************************************************")
# See note in FieldUpdater class in concerto_classes (or the documentation)
# for why this is commented out
##    CasesUpdater = concerto_classes.FieldUpdater("Cases")
##    CasesUpdater.update()
    Cases.tidy_end()
    message("**************************************************")
    LeasesUpdater = concerto_classes.FieldUpdater("Leases")
    LeasesUpdater.update()
    Leases.tidy_end()
    message("**************************************************")
    SitesUpdater = concerto_classes.FieldUpdater("Sites")
    SitesUpdater.update()
    Sites.tidy_end()
    message("**************************************************")
    # End FieldUpdater part
    time.sleep(10)
    for element in os.listdir(MAIN_PATH):
        try:
            shutil.copy2(os.path.join(MAIN_PATH, element),
                         os.path.join(SANDBOX, element))
        except Exception as e:
            if not element.endswith('lock'):
                message("Unable to copy {}. {}".format(element, str(e)))
    message("Sandbox Updated")
# Legacy code here: this is in many ways a better solution that the shutil
# solution above, but the ways network folders are cached was changed in
# spring 2019 which caused this to sometimes fail.
##    Sandbox = concerto_classes.Sandbox()
##    try:
##        Sandbox.update()
##        Sandbox.verify()
##    except Exception as e:
##        message("~ Unable to update Sandbox: {}".format(str(e)))
    end = time.time()
    total_time = end - start
    message("**************************************************")
    message("**************************************************")
    message("Script completed in {}m{}s".format(
        str(int(total_time // 60)), str(int(total_time % 60))).center(50))
    message("**************************************************")
    message("**************************************************")