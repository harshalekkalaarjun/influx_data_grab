import sys
import os
import tkinter
from cx_Freeze import setup, Executable

# --------------------------------------------------
# Function to locate resource files (works for dev and cx_Freeze)
# --------------------------------------------------
def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for cx_Freeze """
    try:
        base_path = sys._MEIPASS  # cx_Freeze temporary folder
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# --------------------------------------------------
# Determine the base for the executable
# --------------------------------------------------
base = "Win32GUI" if sys.platform == "win32" else None

# --------------------------------------------------
# Initialize Tkinter to access Tcl/Tk paths
# --------------------------------------------------
root = tkinter.Tk()
tcl_dir = root.tk.exprstring('$tcl_library')
tk_dir = root.tk.exprstring('$tk_library')
root.destroy()

# --------------------------------------------------
# Define build options
# --------------------------------------------------
build_exe_options = {
    'packages': ['numpy', 'pandas', 'openpyxl', "influxdb"],
    'include_files': [
        'measurements_fields.txt',
        'Influx-DB.ico',  # Include your icon file
        (tcl_dir, os.path.join('lib', 'tcl')),  # Include Tcl library
        (tk_dir, os.path.join('lib', 'tk')),    # Include Tk library
    ],
    'include_msvcr': True,  # Include Microsoft Visual C++ Redistributables
    'excludes': [],
}

# --------------------------------------------------
# Define the executable
# --------------------------------------------------
exe = Executable(
    script="1.0.6_1.py",
    base=base,
    target_name="influx_datacollector.exe",
    icon="Influx-DB.ico"  # Use ICO file for the .exe icon
)

# --------------------------------------------------
# Setup configuration
# --------------------------------------------------
setup(
    name="influx_datacollector",
    version="1.0.3",
    description="A GUI application to influx_datacollector.",
    options={'build_exe': build_exe_options},
    executables=[exe],
)
