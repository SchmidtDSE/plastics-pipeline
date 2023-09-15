import os
import re
import sys

USAGE_STR = 'python clean_filenames.py [directory]'
NUM_ARGS = 1


def clean_name(target_path):
    pieces = target_path.split('/')
    filename = pieces[-1]
    filename_clean = re.sub(r'[^A-Za-z0-9\\.]', '', filename).lower()
    pieces[-1] = filename_clean
    return '/'.join(pieces)


def execute(directory_path):
    all_contents = os.listdir(directory_path)
    all_contents_expand = map(
        lambda x: os.path.join(directory_path, x),
        all_contents
    )
    file_paths = filter(lambda x: os.path.isfile(x), all_contents_expand)
    file_paths_rewrite = map(
        lambda x: (x, clean_name(x)),
        file_paths
    )

    for (prior, new) in file_paths_rewrite:
        os.rename(prior, new)


def main():
    if len(sys.argv) != NUM_ARGS + 1:
        print(USAGE_STR)
        sys.exit(1)

    directory_path = sys.argv[1]
    
    execute(directory_path)


if __name__ == '__main__':
    main()
