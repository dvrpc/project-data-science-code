from pathlib import Path
import platform

DB_NAME = "ucity"

if platform.system() == "Darwin":
    GDRIVE_FOLDER = Path("/Volumes/GoogleDrive/Shared drives/U_City_FY_21")

elif platform.system() == "Windows":
    GDRIVE_FOLDER = Path(r"G:\Shared drives\U_City_FY_21")
