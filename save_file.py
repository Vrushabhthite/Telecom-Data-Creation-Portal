import tempfile
import shutil
import os
import uuid

def save_file(upload_file):

    ext = os.path.splitext(upload_file.filename)[1]  # get .xlsx / .csv

    temp = tempfile.NamedTemporaryFile(delete=False, suffix=ext)

    temp_path = temp.name

    with open(temp_path, "wb") as buffer:
        shutil.copyfileobj(upload_file.file, buffer)

    return temp_path