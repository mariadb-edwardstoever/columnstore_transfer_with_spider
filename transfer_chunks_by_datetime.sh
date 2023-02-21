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

DATETIME_COLUMN=record_create_datetime

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
function print_color () {
  case "$COLOR" in
    default) i="0;36" ;; red)  i="0;31" ;; blue) i="0;34" ;; green) i="0;32" ;; yellow) i="0;33" ;; magenta) i="0;35" ;; cyan) i="0;36" ;; lred) i="1;31" ;; lblue) i="1;34" ;; lgreen) i="1;32" ;; lyellow) i="1;33" ;; lmagenta) i="1;35" ;; lcyan) i="1;36" ;; *) i="0" ;;
  esac
  printf "\033[${i}m${1}\033[0m"
}
COLOR=lgreen
CSsourceConn -ABNe "select now();" >/dev/null 2>/dev/null && print_color "Can connect to source database.\n" || ERR=true
if [ $ERR ]; then COLOR=lred; print_color "Something went wrong connecting to source.\n"; exit; fi
CStargetConn -ABNe "select now();" >/dev/null 2>/dev/null && print_color "Can connect to target database.\n" || ERR=true
if [ $ERR ]; then COLOR=lred; print_color "Something went wrong connecting to target.\n"; exit; fi

if [ "$TRUNCATE_TARGET_TABLE" == "YES" ]; then
  CStargetConn -ABNe "truncate table $TARGET_SCHEMA.$TARGET_TABLE;" &&  echo "Truncated table $TARGET_SCHEMA.$TARGET_TABLE on target database." || ERR=true
  if [ $ERR ]; then echo "Something went wrong during truncate of $TARGET_SCHEMA.$TARGET_TABLE on target."; exit; fi
fi

SQL="select count(*),unix_timestamp(min($DATETIME_COLUMN)),unix_timestamp(max($DATETIME_COLUMN)),(unix_timestamp(max($DATETIME_COLUMN))-unix_timestamp(min($DATETIME_COLUMN))) from $SOURCE_SCHEMA.$SOURCE_TABLE;"

GET_DB_VALUES=$(CSsourceConn -ABNe "$SQL")
TOTAL_ROWS_SOURCE=$(echo $GET_DB_VALUES| awk '{print $1}' |xargs)
FIRST_REPORT_DATETIME_AS_UNIX_TIMESTAMP=$(echo $GET_DB_VALUES| awk '{print $2}'|xargs)
LAST_REPORT_DATETIME_AS_UNIX_TIMESTAMP=$(echo $GET_DB_VALUES| awk '{print $3}'|xargs)
COUNT_ALL_SECONDS=$(echo $GET_DB_VALUES| awk '{print $4}'|xargs)
HOW_MANY_CHUNKS=$((($TOTAL_ROWS_SOURCE/$APPROX_ROWS_PER_CHUNK)+1))
SECONDS_PER_CHUNK=$(($COUNT_ALL_SECONDS/$HOW_MANY_CHUNKS))
echo "Starting to insert rows from table with Spider Engine."

ii=$FIRST_REPORT_DATETIME_AS_UNIX_TIMESTAMP; 
STEP=0;
while [[ $ii -lt $(($LAST_REPORT_DATETIME_AS_UNIX_TIMESTAMP+1)) ]]; do
  STARTSEC=$(date +%s)
  
  if [[ $ii -ge $LAST_REPORT_DATETIME_AS_UNIX_TIMESTAMP ]]; then OFFSET=0; else OFFSET=1; fi

  SQL="insert into $TARGET_SCHEMA.$TARGET_TABLE select * from $SPIDER_SCHEMA.$SPIDER_TABLE where $DATETIME_COLUMN
       between
       from_unixtime($ii) and from_unixtime((($ii + $SECONDS_PER_CHUNK) -$OFFSET)); select format(ROW_COUNT(),0);"

  ROWS_INSERTED=$(CStargetConn -ABNe "$SQL") || ERR=true
  if [ $ERR ]; then echo "Something went wrong during insert."; exit; fi
  ii=$(($ii + $SECONDS_PER_CHUNK))
  STEP=$(($STEP +1))
  ENDSEC=$(date +%s)
  echo "Step $STEP of $(($HOW_MANY_CHUNKS+1)) completed.  $ROWS_INSERTED rows inserted in $(($ENDSEC-$STARTSEC)) seconds."
  unset ROWS_INSERTED STARTSEC ENDSEC ERR
done


TOTAL_ROWS_TARGET=$(CStargetConn -ABNe "select count(*) from $TARGET_SCHEMA.$TARGET_TABLE;")

echo "Count of rows on source: $TOTAL_ROWS_SOURCE"
echo "Count of rows on target: $TOTAL_ROWS_TARGET"
echo "Script completed succesfully."
