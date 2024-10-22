echo "== Installing required software... =="
pip install xlsx2csv

echo "== Gathering OECD GDP projections... =="
wget "https://sdmx.oecd.org/archive/rest/data/OECD,DF_EO114_LTB,/.GDPVD.S0.A?startPeriod=1990&endPeriod=2060&dimensionAtObservation=AllDimensions&format=csvfilewithlabels"
mv .GDPVD.S0.A\?startPeriod\=1990\&endPeriod\=2060\&dimensionAtObservation\=AllDimensions\&format\=csvfilewithlabels gdp_new_format_raw.csv
python oecd_convert_to_legacy_format.py gdp_new_format_raw.csv a1_gdp_raw.csv

echo "== Gathering UN population estimates... =="
wget2 https://population.un.org/wpp/Download/Files/1_Indicator%20\(Standard\)/EXCEL_FILES/1_General/WPP2024_GEN_F01_DEMOGRAPHIC_INDICATORS_COMPACT.xlsx
xlsx2csv WPP2024_GEN_F01_DEMOGRAPHIC_INDICATORS_COMPACT.xlsx -n "Estimates" > wpp_raw.csv
sed 1,17d wpp_raw.csv > a2_population_raw.csv

echo "== Gathering UN population projections... =="
wget2 "https://population.un.org/wpp/Download/Files/2_Indicators%20(Probabilistic)/EXCEL_FILES/2_Population/UN_PPP2024_Output_PopTot.xlsx"
xlsx2csv UN_PPP2024_Output_PopTot.xlsx -n "Median" > wpp_future_raw.csv
sed 1,17d wpp_future_raw.csv > wpp_future_raw_cut.csv
python linearize_un_future.py wpp_future_raw_cut.csv a4_pop_projection.csv

echo "== Gathering raw mass flows... =="
wget https://global-plastics-tool.org/data/raw_data_in_slim.zip
unzip raw_data_in_slim.zip
mv data raw_data_in
mv raw_data_in/*.csv ./
mv raw_data_in/*.json ./

echo "== Get data complete =="
