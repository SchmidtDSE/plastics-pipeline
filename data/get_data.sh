echo "== Installing required software... =="
pip install xlsx2csv
sudo apt-get install wget2

echo "== Gathering UN population estimates... =="
wget2 https://population.un.org/wpp/Download/Files/1_Indicator%20\(Standard\)/EXCEL_FILES/1_General/WPP2024_GEN_F01_DEMOGRAPHIC_INDICATORS_COMPACT.xlsx
xlsx2csv WPP2024_GEN_F01_DEMOGRAPHIC_INDICATORS_COMPACT.xlsx -s 1 > wpp_raw.csv
sed 1,17d wpp_raw.csv > a2populationraw.csv

echo "== Gathering OECD GDP projections... =="
wget "https://sdmx.oecd.org/archive/rest/data/OECD,DF_EO114_LTB,/.GDPVD.S0.A?startPeriod=1990&endPeriod=2060&dimensionAtObservation=AllDimensions&format=csvfilewithlabels"
mv .GDPVD.S0.A\?startPeriod\=1990\&endPeriod\=2060\&dimensionAtObservation\=AllDimensions\&format\=csvfilewithlabels gdp_new_format_raw.csv
python oecd_convert_to_legacy_format.py gdp_new_format_raw.csv a1gdpraw.csv

echo "== Gathering raw mass flows... =="
wget https://global-plastics-tool.org/data/raw_data_in_slim.zip
unzip raw_data_in_slim.zip
mv data raw_data_in
mv raw_data_in/*.csv ./
mv raw_data_in/*.json ./

echo "== Get data complete =="
