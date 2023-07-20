cd ../output

rm -f -- combined.db

cp ../data/02_Data_Raw/18_Net_Trade_China.csv ./
cp ../data/02_Data_Raw/19_Net_Trade_NAFTA.csv ./
cp ../data/02_Data_Raw/20_Net_Trade_EU.csv ./
cp ../data/02_Data_Raw/21_Net_Trade_Row.csv ./

cat ../sql/01_import_files.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs.sql | sqlite3 combined.db
cat ../sql/03_views.sql | sqlite3 combined.db
cat ../sql/04_combine.sql | sqlite3 combined.db
cat ../sql/05_export.sql | sqlite3 combined.db
