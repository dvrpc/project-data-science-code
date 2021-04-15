from pathlib import Path
import platform

if platform.system() == "Darwin":
    GDRIVE_BASE = Path("/Volumes/GoogleDrive/My Drive")

elif platform.system() == "Windows":
    GDRIVE_BASE = Path(r"G:\My Drive")
