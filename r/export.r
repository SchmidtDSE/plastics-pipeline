output_dir <- file.path(dirname(getwd()), "output")

# Sam's ask 1 = Waste Gen - year, region, market_sector, polymer type
# Waste_Gen_2 is waste for each polymer type in Million Metric Tons
write.csv(KD3, file=glue("{output_dir}/waste_gen.csv"))

# Sam's ask 2 = Consumption - year, region, market_sector
# PC_sector is consumption in Million Metric Tons
write.csv(TB4, file=glue("{output_dir}/consumption.csv"))

# Sam's ask 3 = Waste Gen - year, region, EOL type
# EOL_Waste_5 is waste for each EOL fate in Million Metric Tons
write.csv(TBA6, file=glue("{output_dir}/eol.csv"))
