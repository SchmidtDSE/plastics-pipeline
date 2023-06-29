echo "== Import files (1/5) =="
cp -r ../data ../data_workspace
python3 01_import_files/clean_filenames.py ../data_workspace
cd ../data_workspace
cat ../sql/01_import_files/import_files.sql | sqlite3 combined.db

echo "== Clean inputs (2/5) =="
cat ../sql/02_clean_inputs/raw_additives.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_end_use_china.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_end_use_eu30.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_end_use_nafta.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_end_use_row.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_eol_china.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_eol_eu30.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_eol_nafta.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_eol_row.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_net_import_articles.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_net_import_fibers.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_net_import_finished_goods.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_net_import_resin.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_net_trade_china.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_net_trade_eu30.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_net_trade_nafta.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_net_trade_row.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_production_fiber.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_production_resin.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs/raw_meta_sectors.sql | sqlite3 combined.db

echo "== Build views (3/5) =="
cat ../sql/03_views/consumption.sql | sqlite3 combined.db
cat ../sql/03_views/end_use.sql | sqlite3 combined.db
cat ../sql/03_views/eol.sql | sqlite3 combined.db
cat ../sql/03_views/input_additives.sql | sqlite3 combined.db
cat ../sql/03_views/input_import.sql | sqlite3 combined.db
cat ../sql/03_views/input_production.sql | sqlite3 combined.db
cat ../sql/03_views/inputs.sql | sqlite3 combined.db
cat ../sql/03_views/overview_consumption.sql | sqlite3 combined.db
cat ../sql/03_views/overview_end_use.sql | sqlite3 combined.db
cat ../sql/03_views/overview_eol.sql | sqlite3 combined.db
cat ../sql/03_views/overview_inputs.sql | sqlite3 combined.db
cat ../sql/03_views/summary_percents.sql | sqlite3 combined.db
cat ../sql/03_views/summary.sql | sqlite3 combined.db
mv combined.db ../output_sql

echo "== Build frame (4/5) =="
cd ../output_sql
cat ../sql/04_frame/export.sql | sqlite3 combined.db
cat ../sql/04_frame/export_percents.sql | sqlite3 combined.db
python3 ../sql/check_summary_percents.py ./summary_percents.csv || exit 1;

echo "== Projections (5/5) =="
python3 ../sql/05_project/project.py ../output_sql/summary.csv ../output_sql/extrapolate.csv ../sql/05_project/params.json
python3 ../sql/05_project/project.py ../output_sql/summary_percents.csv ../output_sql/extrapolate_percents.csv ../sql/05_project/params.json
python3 ../sql/check_summary_percents.py ../output_sql/extrapolate_percents.csv || exit 1;
cat ../sql/05_project/add_files.sql | sqlite3 combined.db
cat ../sql/05_project/extrapolate.sql | sqlite3 combined.db
cat ../sql/05_project/extrapolate_percents.sql | sqlite3 combined.db
cat ../sql/05_project/export_final.sql | sqlite3 combined.db
python3 ../sql/check_summary_percents.py ../output_sql/web.csv || exit 1;
