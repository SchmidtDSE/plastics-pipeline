# Remove intermediate files such that the pipeline runs from scratch.
#
# License: BSD, see LICENSE.md

rm -r data_workspace
rm -r output
rm -r task
mkdir task
cp job_template.json task/job.json
