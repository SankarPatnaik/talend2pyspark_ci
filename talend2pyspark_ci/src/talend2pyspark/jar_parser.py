import zipfile
import os


def extract_metadata(jar_path):
    """Extract metadata XML/JSON from Talend JAR"""
    extracted_files = []
    with zipfile.ZipFile(jar_path, 'r') as jar:
        for file in jar.namelist():
            if file.endswith('.item') or file.endswith('.xml') or file.endswith('.json'):
                extract_path = os.path.join('/tmp', os.path.basename(file))
                jar.extract(file, '/tmp')
                extracted_files.append(extract_path)
    return {"files": extracted_files}
