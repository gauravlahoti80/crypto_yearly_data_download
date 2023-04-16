import zipfile
import os

def unzip(directory):
    for files in os.listdir(str(directory)):
        with zipfile.ZipFile(str(directory) + "/" + files, "r") as zip_file:
            zip_file.extractall(str(directory))
