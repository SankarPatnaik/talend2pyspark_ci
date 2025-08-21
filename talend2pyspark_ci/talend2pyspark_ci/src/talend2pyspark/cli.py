import argparse
from . import jar_parser, mapping, codegen


def main():
    parser = argparse.ArgumentParser(description='Talend JAR to PySpark converter')
    parser.add_argument('--jar', required=True, help='Path to Talend JAR file')
    parser.add_argument('--output', required=True, help='Output directory for PySpark code')
    args = parser.parse_args()

    metadata = jar_parser.extract_metadata(args.jar)
    plan = mapping.convert_metadata_to_plan(metadata)
    codegen.write_pyspark_code(plan, args.output)

if __name__ == '__main__':
    main()
