import csv
import sys

USAGE = 'python oecd_convert_to_legacy_format.py [input] [output]'
NUM_ARGS = 2


def transform_row(target):
    return {
        'LOCATION': target['LOCATION'],
        'TIME': target['TIME_PERIOD'],
        'Value': float(target['OBS_VALUE']) / 1000000
    }


def main():
    if len(sys.argv) != NUM_ARGS + 1:
        print(USAGE)
        return
  
    source_loc = sys.argv[1]
    output_loc = sys.argv[2]

    with open(source_loc) as f_in:
        with open(output_loc, 'w') as f_out:
            reader = csv.DictReader(f_in)
            writer = csv.DictWriter(f_out, fieldnames=['LOCATION', 'TIME', 'Value'])
            transformed = map(transform_row, reader)
            writer.writeheader()
            writer.writerows(transformed)


if __name__ == '__main__':
    main()
