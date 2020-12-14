from pathlib import Path
import platform

DB_NAME = "downingtown"
_folder_name = "Downingtown Area Transportation Study"


if platform.system() == "Darwin":
    GDRIVE_FOLDER = Path(f"/Volumes/GoogleDrive/Shared drives/{_folder_name}")

elif platform.system() == "Windows":
    GDRIVE_FOLDER = Path(fr"G:\Shared drives\{_folder_name}")
