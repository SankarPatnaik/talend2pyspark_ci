import zipfile
import os
import xml.etree.ElementTree as ET
import json

# Component Mapping
COMPONENT_MAPPING = {
    "tFileInputDelimited": "read_csv",
    "tFileInputJSON": "read_json",
    "tFileInputParquet": "read_parquet",
    "tFilterRow": "filter",
    "tAggregateRow": "aggregate",
    "tJoin": "join",
    "tFileOutputDelimited": "write_csv",
    "tFileOutputParquet": "write_parquet"
}

def extract_talend_metadata(jar_path, output_dir="talend_job_extracted"):
    """ Extract metadata (Step 1) """
    if not zipfile.is_zipfile(jar_path):
        raise ValueError(f"{jar_path} is not a valid JAR/ZIP file")

    with zipfile.ZipFile(jar_path, 'r') as jar:
        jar.extractall(output_dir)
        print(f"[INFO] Extracted JAR to {output_dir}")

    metadata = {"properties": {}, "components": []}

    # Parse .properties files
    for root, _, files in os.walk(output_dir):
        for file in files:
            if file.endswith(".properties"):
                props_path = os.path.join(root, file)
                with open(props_path, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        if "=" in line:
                            key, value = line.strip().split("=", 1)
                            metadata["properties"][key.strip()] = value.strip()

    # Parse .item / .xml files
    for root, _, files in os.walk(output_dir):
        for file in files:
            if file.endswith(".item") or file.endswith(".xml"):
                xml_path = os.path.join(root, file)
                try:
                    tree = ET.parse(xml_path)
                    root_el = tree.getroot()
                    for comp in root_el.findall(".//node"):
                        component_name = comp.attrib.get("componentName")
                        component_id = comp.attrib.get("id")
                        component = {"id": component_id, "name": component_name, "properties": {}}
                        for elem in comp.findall("elementParameter"):
                            key = elem.attrib.get("name")
                            value = elem.attrib.get("value")
                            component["properties"][key] = value
                        metadata["components"].append(component)
                except Exception as e:
                    print(f"[WARN] Could not parse {xml_path}: {e}")

    # Save metadata as JSON
    output_json = os.path.join(output_dir, "talend_metadata.json")
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4)

    return metadata


def generate_pyspark_code(metadata, output_file="generated_pyspark_job.py"):
    """ Step 2: Convert Talend metadata → PySpark skeleton """
    code_lines = [
        "from pyspark.sql import SparkSession",
        "",
        "spark = SparkSession.builder.appName('TalendJob_Migrated').getOrCreate()",
        ""
    ]

    df_counter = 1
    df_map = {}  # maps component id → dataframe variable

    for comp in metadata["components"]:
        comp_name = comp["name"]
        props = comp["properties"]
        action = COMPONENT_MAPPING.get(comp_name)

        if action == "read_csv":
            path = props.get("FILE_NAME", "/path/to/input.csv")
            code_lines.append(f"df{df_counter} = spark.read.option('header','true').csv('{path}')")
            df_map[comp["id"]] = f"df{df_counter}"
            df_counter += 1

        elif action == "read_json":
            path = props.get("FILE_NAME", "/path/to/input.json")
            code_lines.append(f"df{df_counter} = spark.read.json('{path}')")
            df_map[comp["id"]] = f"df{df_counter}"
            df_counter += 1

        elif action == "read_parquet":
            path = props.get("FILE_NAME", "/path/to/input.parquet")
            code_lines.append(f"df{df_counter} = spark.read.parquet('{path}')")
            df_map[comp["id"]] = f"df{df_counter}"
            df_counter += 1

        elif action == "filter":
            cond = props.get("CONDITION", "1=1")
            last_df = f"df{df_counter-1}"
            code_lines.append(f"df{df_counter} = {last_df}.filter('{cond}')")
            df_map[comp["id"]] = f"df{df_counter}"
            df_counter += 1

        elif action == "write_csv":
            path = props.get("FILE_NAME", "/path/to/output.csv")
            last_df = f"df{df_counter-1}"
            code_lines.append(f"{last_df}.write.mode('overwrite').csv('{path}')")

        elif action == "write_parquet":
            path = props.get("FILE_NAME", "/path/to/output.parquet")
            last_df = f"df{df_counter-1}"
            code_lines.append(f"{last_df}.write.mode('overwrite').parquet('{path}')")

        else:
            code_lines.append(f"# TODO: Unsupported component {comp_name}")

    code_lines.append("\nspark.stop()")

    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(code_lines))

    print(f"[INFO] PySpark code generated: {output_file}")
    return output_file


# Example usage
if __name__ == "__main__":
    jar_file = "my_talend_job.jar"  # Replace with actual Talend JAR
    metadata = extract_talend_metadata(jar_file)
    generate_pyspark_code(metadata)
