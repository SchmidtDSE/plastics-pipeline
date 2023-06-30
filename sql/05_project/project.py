import csv
import json
import math
import sys

import localreg
import numpy
import sklearn.linear_model
import sklearn.metrics

USAGE_STR = 'python project.py [input] [output] [params]'
NUM_ARGS = 3


class Params:

    def __init__(self, params_raw):
        self._strategy = params_raw['strategy']

        self._print_r2 = params_raw['printR2']

        self._kernel = params_raw.get('kernel', None)
        self._degree = params_raw.get('degree', None)
        self._radius = params_raw.get('radius', None)
        self._frac = params_raw.get('frac', None)
        
        self._partition_attr = params_raw['partition']
        self._input_attr = params_raw['input']

        default_ignore_attrs = {self._partition_attr, self._input_attr}
        extra_ingore_attrs = set(params_raw['ignore'])
        self._ignore_attrs = extra_ingore_attrs.union(default_ignore_attrs)

        self._min_input = params_raw['minInput']
        self._max_input = params_raw['maxInput']

        self._min_zero_attrs = params_raw['minZero']
        self._percent_groups = params_raw['percentGroups']
    
    def get_strategy(self):
        return self._strategy

    def get_print_r2(self):
        return self._print_r2

    def get_kernel(self):
        return self._kernel
    
    def get_degree(self):
        return self._degree
    
    def get_radius(self):
        return self._radius
    
    def get_frac(self):
        return self._frac
    
    def get_partition_attr(self):
        return self._partition_attr
    
    def get_input_attr(self):
        return self._input_attr
    
    def get_ignore_attrs(self):
        return self._ignore_attrs
    
    def get_min_input(self):
        return self._min_input
    
    def get_max_input(self):
        return self._max_input
    
    def get_min_zero_attrs(self):
        return self._min_zero_attrs
    
    def get_percent_groups(self):
        return self._percent_groups


class ModelBuilder:

    def fit(self, name, x, y, targets):
        raise NotImplementedError('Use implementor.')


class LinearModelBuilder:

    def __init__(self, params):
        self._print_r2 = params.get_print_r2()

    def fit(self, name, x, y, targets):
        x_nested = self._nest(x)
        targets_nested = self._nest(targets)

        model = sklearn.linear_model.LinearRegression()
        model.fit(x_nested, y)

        if self._print_r2:
            y_pred = model.predict(x_nested)
            r2 = sklearn.metrics.r2_score(y, y_pred)
            print('%s R2: %f' % (name, r2))

        return model.predict(targets_nested)

    def _nest(self, target):
        return [[d] for d in target]



class LoessModelBuilder:

    def __init__(self, params):
        kernel_name = params.get_kernel()
        self._degree = params.get_degree()
        self._radius = params.get_radius()
        self._frac = params.get_frac()

        self._kernel = {
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
        }[kernel_name]

    def fit(self, name, x, y, targets):
        return localreg.localreg(
            numpy.array(x),
            numpy.array(y),
            x0=numpy.array(targets),
            degree=self._degree,
            kernel=self._kernel,
            radius=self._radius,
            frac=self._frac
        )


def build_model_builder(params):
    strategy = params.get_strategy()
    builder_builder = {
        'linear': lambda x: LinearModelBuilder(x),
        'loess': lambda x: LoessModelBuilder(x)
    }[strategy]
    return builder_builder(params)


def is_valid_num(target):
    return target is not None and math.isfinite(target)


def get_partition(input_rows, partition_key, partition_attr):
    return filter(lambda x: x[partition_attr] == partition_key, input_rows)


def is_complete(target):
    all_attrs = [target['input'], target['response']]
    invalid_attrs = filter(lambda x: not is_valid_num(x), all_attrs)
    num_invalid = sum(map(lambda x: 1, invalid_attrs))
    return num_invalid == 0


def try_float(target):
    try:
        return float(target)
    except ValueError:
        return None
    except TypeError:
        return None


def try_int(target):
    try:
        return int(target)
    except ValueError:
        return None
    except TypeError:
        return None


def get_inputs_outputs(input_rows, input_attr, response):
    return map(lambda x: {
        'input': x[input_attr],
        'response': x[response]
    }, input_rows)


def parse(input_rows_raw, params):
    input_rows = list(input_rows_raw)

    partition_attr = params.get_partition_attr()
    input_attr = params.get_input_attr()

    for row in input_rows:
        for attr in row:
            if attr == partition_attr:
                new_value = row[attr]
            elif attr == input_attr:
                new_value = try_int(row[attr])
            else:
                new_value = try_float(row[attr])

            row[attr] = new_value

    return input_rows


def expand(input_rows, params):
    input_attr = params.get_input_attr()

    min_input = params.get_min_input()
    max_input = params.get_max_input()
    get_years = lambda: set(range(min_input, max_input + 1))
    
    partition_attr = params.get_partition_attr()
    partition_keys = set(map(lambda x: x[partition_attr], input_rows))

    sample_row = input_rows[0]
    all_attrs = set(sample_row.keys()) - {input_attr, partition_attr}

    new_outputs = []
    for partition_key in partition_keys:
        partition_iter = get_partition(input_rows, partition_key, partition_attr)
        inputs_represented = set(map(lambda x: x[input_attr], input_rows))
        missing_inputs = get_years() - inputs_represented

        for missing_input in missing_inputs:
            new_record = {
                input_attr: missing_input,
                partition_attr: partition_key
            }
            
            for response in all_attrs:
                new_record[response] = None

            new_outputs.append(new_record)

    return input_rows + new_outputs


def predict(input_rows, model_builder, params):
    input_attr = params.get_input_attr()

    sample_row = input_rows[0]
    modelable_attrs = set(sample_row.keys()) - params.get_ignore_attrs()

    partition_attr = params.get_partition_attr()
    partition_keys = set(map(lambda x: x[partition_attr], input_rows))

    for partition_key in partition_keys:
        partition_iter = get_partition(input_rows, partition_key, partition_attr)
        partition = list(partition_iter)

        for response in modelable_attrs:
            inputs_outputs_iter = get_inputs_outputs(partition, input_attr, response)
            inputs_outputs = list(inputs_outputs_iter)

            inputs_outputs_complete = list(filter(is_complete, inputs_outputs))
            inputs = [x['input'] for x in inputs_outputs_complete]
            responses = [x['response'] for x in inputs_outputs_complete]

            not_complete = lambda x: not is_complete(x)
            inputs_outputs_incomplete = filter(not_complete, inputs_outputs)
            inputs_incomplete = [x['input'] for x in inputs_outputs_incomplete]

            name = partition_key + ' ' + response
            missing_outputs = model_builder.fit(name, inputs, responses, inputs_incomplete)
            missing_outputs_by_input = dict(zip(inputs_incomplete, missing_outputs))

            partition_rows_incomplete = filter(
                lambda x: not is_valid_num(x[response]),
                partition
            )
            
            for input_row in partition_rows_incomplete:
                input_val = input_row[input_attr]
                new_val = missing_outputs_by_input[input_val]
                input_row[response] = new_val

    return input_rows


def enforce_min_zero(target, params):
    for row in target:
        attrs = params.get_min_zero_attrs()
        attrs_found = filter(lambda x: x in row, attrs)
        for attr in attrs_found:
            value = row[attr]
            if value < 0:
                row[attr] = 0

    return target


def enforce_perecent_groups(target, params):
    for row in target:
        groups = params.get_percent_groups()
        groups_found = filter(lambda x: x[0] in row, groups)
        for group in groups_found:
            total = sum(map(lambda x: row[x], group))
            for attr in group:
                row[attr] = row[attr] / total

    return target


def main():
    if len(sys.argv) != NUM_ARGS + 1:
        print(USAGE_STR)
        sys.exit(1)

    input_loc = sys.argv[1]
    output_loc = sys.argv[2]
    params_loc = sys.argv[3]

    with open(params_loc) as f:
        params = Params(json.load(f))
        model_builder = build_model_builder(params)

    with open(input_loc) as f:
        input_rows_raw = csv.DictReader(f)
        input_rows_parsed = parse(input_rows_raw, params)
        input_rows = list(input_rows_parsed)

    expanded_rows = expand(input_rows, params)

    rows_with_prediction = predict(expanded_rows, model_builder, params)

    rows_with_min = enforce_min_zero(rows_with_prediction, params)
    output_rows = enforce_perecent_groups(rows_with_min, params)

    with open(output_loc, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=output_rows[0].keys())
        writer.writeheader()
        writer.writerows(output_rows)


if __name__ == '__main__':
    main()