#!/bin/bash

# TABLES ON SOURCE AND TARGET:
# CREATE TABLE `amz_narrow_test` (
#   `report_date` date NOT NULL,
#   `record_id` bigint(20) DEFAULT NULL,
#   `record_create_datetime` datetime NOT NULL  
# ) ENGINE=Columnstore DEFAULT CHARSET=utf8mb3;

# SPIDER TABLE ON TARGET THAT CONNECTS TO SOURCE:
# CREATE TABLE `remote_amz_narrow_test` (
#   `report_date` date,
#   `record_id` bigint(20),
#   `record_create_datetime` datetime 
# ) ENGINE=Spider COMMENT='wrapper "mariadb", srv "main_schema_on_db0000xxxx", table "amz_narrow_test"';

# Script by Edward Stoever for MariaDB Support Ref: CS0514319
# Use this script to transfer data from one Columnstore table to another using Spider engine to connect to Remote DB.
# Spider table that connects to source is always defined on Target DB. 
# This script is designed for use on SkySQL databases. 

# REVEIW THE VALUES AND EDIT ACCORDINGLY

#### START USER DEFINED VALUES
SOURCE_SCHEMA=main
SOURCE_TABLE=amz_narrow_test

TARGET_SCHEMA=main
TARGET_TABLE=amz_narrow_test

SPIDER_SCHEMA=main
SPIDER_TABLE=remote_amz_narrow_test

DATE_COLUMN=report_date

# Define next variable as YES or NO
TRUNCATE_TARGET_TABLE=YES

# Columnstore is optimized to cpimport 8 million rows at a time.
# You can adjust the number according to the time it takes to complete each chunk. 
APPROX_ROWS_PER_CHUNK=8000000

# DO NOT RUN AGGREGATE QUERIES ON SPIDER TABLES. DEFINE SOURCE AND TARGET
shopt -s expand_aliases
alias CSsourceConn='mariadb --host source-db.mdb0001908.db1.skysql.net --port 5001 --user DB000000xx -p"yyE.WccxQtLJzhRI8FB2Xy" --ssl-ca ~/aws_skysql_chain.pem'
alias CStargetConn='mariadb --host target-db.mdb0001908.db1.skysql.net --port 5001 --user DB000000yy -p"WAu3jK^C8IAbvbXhKznWKp0" --ssl-ca ~/aws_skysql_chain.pem'

#### END USER DEFINED VALUES

unset ERR
CSsourceConn -ABNe "select now();" >/dev/null 2>/dev/null && echo "Can connect to source database." || ERR=true
if [ $ERR ]; then echo "Something went wrong connecting to source."; exit; fi
CStargetConn -ABNe "select now();" >/dev/null 2>/dev/null && echo "Can connect to target database." || ERR=true
if [ $ERR ]; then echo "Something went wrong connecting to target."; exit; fi

if [ "$TRUNCATE_TARGET_TABLE" == "YES" ]; then
  CStargetConn -ABNe "truncate table $TARGET_SCHEMA.$TARGET_TABLE;" &&  echo "Truncated table $TARGET_SCHEMA.$TARGET_TABLE on target database." || ERR=true
  if [ $ERR ]; then echo "Something went wrong during truncate of $TARGET_SCHEMA.$TARGET_TABLE on target."; exit; fi
fi

TOTAL_ROWS_SOURCE=$(CSsourceConn -ABNe "select count(*) from $SOURCE_SCHEMA.$SOURCE_TABLE;")

# HOW_MANY_SLICES=$(($TOTAL_ROWS_SOURCE/$APPROX_ROWS_PER_CHUNK))
HOW_MANY_SLICES=$(($TOTAL_ROWS_SOURCE/$APPROX_ROWS_PER_CHUNK + + ( $TOTAL_ROWS_SOURCE % $APPROX_ROWS_PER_CHUNK > 0 )))
FIRST_REPORT_DATE_AS_UNIX_TIMESTAMP=$(CSsourceConn -ABNe "select unix_timestamp(min($DATE_COLUMN)) from $SOURCE_SCHEMA.$SOURCE_TABLE;")
COUNT_ALL_DAYS=$(CSsourceConn -ABNe "select datediff(max($DATE_COLUMN),min($DATE_COLUMN)) from $SOURCE_SCHEMA.$SOURCE_TABLE;") 
DAYS_PER_CHUNK=$(($COUNT_ALL_DAYS/$HOW_MANY_SLICES))

echo "Starting to insert rows from table with Spider Engine."

# while ii is less than or equal to:
ii=0; while [ $ii -le "$HOW_MANY_SLICES" ]; do
  STARTSEC=$(date +%s)
  # AA and BB are offsets
  if [ "$ii" == "0" ]; then AA=0; else AA=1; fi
  if [ "$ii" == "$HOW_MANY_SLICES" ]; then BB=1; else BB=0; fi

  SQL="insert into $TARGET_SCHEMA.$TARGET_TABLE select * from $SPIDER_SCHEMA.$SPIDER_TABLE where $DATE_COLUMN
  between
    from_unixtime($FIRST_REPORT_DATE_AS_UNIX_TIMESTAMP) + interval ( $((${ii}*$DAYS_PER_CHUNK)) + $AA ) day
  and
    from_unixtime($FIRST_REPORT_DATE_AS_UNIX_TIMESTAMP) + interval ( $(((${ii}+1)*$DAYS_PER_CHUNK)) + $BB ) day;"

  CStargetConn -ABNe "$SQL" || ERR=true
  if [ $ERR ]; then echo "Something went wrong during insert."; exit; fi
  ii=$(($ii + 1))
  ENDSEC=$(date +%s)
  echo "Step ${ii} of $(($HOW_MANY_SLICES+1)) completed. Approximately $APPROX_ROWS_PER_CHUNK rows inserted in $(($ENDSEC-$STARTSEC)) seconds."
done

TOTAL_ROWS_TARGET=$(CStargetConn -ABNe "select count(*) from $TARGET_SCHEMA.$TARGET_TABLE;")

echo "Count of rows on source: $TOTAL_ROWS_SOURCE"
echo "Count of rows on target: $TOTAL_ROWS_TARGET"
echo "Script completed succesfully."
