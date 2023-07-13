rm -f -- pipeline.R

Rscript make_script.R

sed -i -E "s/one_drive_path <- [^\\n]+/one_drive_path <- file.path(dirname(getwd()), \"data\")/" pipeline.R

cat export.r >> pipeline.R