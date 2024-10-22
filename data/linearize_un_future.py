import csv
import itertools
import sys

USAGE_STR = 'python linearize_un_future.py [input] [output]'
NUM_ARGS = 2

OUTPUT_COLS = [
    'ISO2 Alpha-code',
    'SDMX code**',
    'Type',
    'Parent code',
    'Year',
    'Total Population, as of 1 January (thousands)'
]


def transform_row(target):
    def get_for_year(year):
        return {
            'ISO2 Alpha-code': target['ISO2 Alpha-code'],
            'ISO3 Alpha-code': target['ISO3 Alpha-code'],
            'SDMX code**': target['SDMX code**'],
            'Type': target['Type'],
            'Parent code': target['Parent code'],
            'Year': year,
            'Total Population, as of 1 January (thousands)': target[str(year)]
        }
    
    years = range(2024, 2100)
    records = map(get_for_year, years)
    return records


def main():
    if len(sys.argv) != NUM_ARGS + 1:
        print(USAGE_STR)
        return
    
    input_loc = sys.argv[1]
    output_loc = sys.argv[2]
    
    with open(input_loc) as f_in:
        reader = csv.DictReader(f_in)
        
        with open(output_loc, 'w') as f_out:
            writer = csv.DictWriter(f_out, fieldnames=OUTPUT_COLS)
            writer.writeheader()

            rows_nested = map(transform_row, reader)
            rows_flat = itertools.chain(*rows_nested)
            writer.writerows(rows_flat)


if __name__ == '__main__':
    main()
