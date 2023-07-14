cd ../output

rm -f -- combined.db

cat ../sql/01_import_files.sql | sqlite3 combined.db
cat ../sql/02_clean_inputs.sql | sqlite3 combined.db
cat ../sql/03_views.sql | sqlite3 combined.db
cat ../sql/04_combine.sql | sqlite3 combined.db
cat ../sql/05_export.sql | sqlite3 combined.db
