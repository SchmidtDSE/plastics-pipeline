# Check that pipeline expected output is present.
#
# License: BSD, see LICENSE.md

[ -f "output/overview_ml.csv" ] || exit 1
[ -f "output/overview_curve.csv" ] || exit 2
[ -f "output/overview_naive.csv" ] || exit 3