import csv
import json
import sys

import localreg

USAGE_STR = 'python project.py [input] [output] [params]'
NUM_ARGS = 3


def build_model_func(params):
    kernel = {
        'rectangular': localreg.rbf.rectangular,
        'triangular': localreg.rbf.triangular,
        'epanechnikov': localreg.rbf.epanechnikov,
        'biweight': localreg.rbf.biweight,
        'triweight': localreg.rbf.triweight,
        'tricube': localreg.rbf.tricube,
        'gaussian': localreg.rbf.gaussian,
        'cosine': localreg.rbf.cosine,
        'logistic': localreg.rbf.logistic,
        'sigmoid': localreg.rbf.sigmoid,
        'silverman': localreg.rbf.silverman
    }[params['kernel']]
    degree = params['degree']
    radius = params['radius']
    
    def inner(x, y):
        return localreg(
            x,
            y,
            degree=degree,
            kernel=kernel,
            radius=radius,
        )

    return inner


def main():
    if len(sys.argv) != NUM_ARGS + 1:
        print(USAGE_STR)
        sys.exit(1)

    input_loc = sys.argv[1]
    output_loc = sys.argv[2]
    params_loc = sys.argv[3]

    with open(params_loc) as f:
        params = json.load(f)
        build_model = build_model_func(params)

    


if __name__ == '__main__'
