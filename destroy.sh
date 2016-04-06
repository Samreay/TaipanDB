dropdb --if-exists taipandb
dropuser --if-exists taipan
createdb -h localhost -p 5432 -U postgres taipandb
psql -U postgres -h 127.0.0.1 -d taipandb -c "CREATE USER taipan WITH PASSWORD 'prototype'; GRANT ALL PRIVILEGES ON DATABASE taipandb TO taipan;"
