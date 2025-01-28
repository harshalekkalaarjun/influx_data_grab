import sys
import os
import tkinter
from cx_Freeze import setup, Executable

# Function to locate resource files
def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for cx_Freeze """
    try:
        # cx_Freeze creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# Determine the base for the executable
base = "Win32GUI" if sys.platform == "win32" else None

# Initialize Tkinter to access Tcl/Tk paths
root = tkinter.Tk()
tcl_dir = root.tk.exprstring('$tcl_library')
tk_dir = root.tk.exprstring('$tk_library')
root.destroy()  # Close the Tkinter window as we don't need it here

# Define build options
build_exe_options = {
    'packages': ['numpy', 'pandas', 'openpyxl'],
    'include_files': [
        # 't4.csv',
        # "p8-data check list 31-7-24.xlsx",
        (tcl_dir, os.path.join('lib', 'tcl')),  # Include Tcl library
        (tk_dir, os.path.join('lib', 'tk')),    # Include Tk library
        # 'app_icon.ico',  # Optional: Include your application icon
    ],
    'include_msvcr': True,  # Include Microsoft Visual C++ Redistributables
    'excludes': [],  # Do not exclude essential modules
}

# Define the executable
exe = Executable(
    script="1.0.7.py",
    base=base,  # Prevents a console window from appearing for GUI apps
    target_name="reportgeneratot.exe",
    # icon="app_icon.ico"  # Optional: Ensure this file exists or remove this parameter
)

# Setup configuration
setup(
    name="reportgeneratot",
    version="1.0.3",
    description="A GUI application to reportgeneratot.",
    options={'build_exe': build_exe_options},
    executables=[exe],
)
