output_dir <- file.path(dirname(getwd()), "output")

write.csv(KD2, file=glue("{output_dir}/KD2.csv"))
write.csv(KA3, file=glue("{output_dir}/KA3.csv"))
write.csv(MA6, file=glue("{output_dir}/MA6.csv"))
write.csv(MA7, file=glue("{output_dir}/MA7.csv"))