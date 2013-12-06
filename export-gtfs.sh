psql -d ridprod -f sql_schema/clean.sql
psql -d ridprod -f sql_schema/fix_bisonstoparea.sql
psql -d ridprod -f exporters/fixes.sql
psql -d ridprod -f exporters/gtfs.sql
zip -9 -j /mnt/kv1/gtfs/new/gtfs-nl.zip /tmp/agency.txt /tmp/calendar_dates.txt /tmp/fare_attributes.txt \
          /tmp/fare_rules.txt /tmp/feed_info.txt /tmp/routes.txt /tmp/shapes.txt \
          /tmp/stop_times.txt /tmp/stops.txt /tmp/transfers.txt /tmp/trips.txt
