--
-- This query set is for the data-set of traffic data in Aarhus, Denmark
--
-- you can found the raw-data at:
--   http://iot.ee.surrey.ac.uk:8080/datasets.html#traffic
--
DROP TABLE IF EXISTS tr_metadata;
CREATE TABLE tr_metadata (
point_1_street          text,
duration_in_sec         real,
point_1_name            int,
point_1_city            text,
point_2_name            int,
point_2_lng             real,
point_2_street          text,
ndt_in_kmh              real,
point_2_postal_code     int,
point_2_country         text,
point_1_street_number   text,
organisation            text,
point_1_lat             real,
point_2_lat             real,
point_1_postal_code     int,
point_2_street_number   text,
point_2_city            text,
ext_id                  int,
road_type               text,
point_1_lng             real,
report_id               int primary key,
point_1_country         text,
distance_in_meters      real,
report_name             text,
rba_id                  int,
_id                     int
);
--
-- LOAD the metadata using the command below:
--
-- cat trafficMetaData.csv | psql traffic -c 'COPY tr_metadata stdin WITH (format CSV, HEADER true)'
--

DROP TABLE IF EXISTS tr_rawdata;
CREATE TABLE tr_rawdata (
status               text,
avg_measured_time    real,
avg_speed            real,
ext_id               int,
median_measured_time real,
timestamp            timestamp,
vehicle_count        real,
_id                  int,
report_id            int
);
CREATE INDEX report_id_on_tr_rawdata ON tr_rawdata(report_id);

--
-- LOAD the data using the shell script below:
--
-- for x in *.csv;
-- do
--   cat $x | psql traffic -c 'COPY tr_rawdata FROM stdin WITH (FORMAT CSV, HEADER true)'
-- done
--
