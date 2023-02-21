#!/bin/bash

#### START USER DEFINED VALUES
SOURCE_SCHEMA=main
SOURCE_TABLE=amz_narrow_test

TARGET_SCHEMA=main
TARGET_TABLE=amz_narrow_test

SPIDER_SCHEMA=main
SPIDER_TABLE=remote_amz_narrow_test

DATE_COLUMN=report_date

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


# TOTAL_ROWS_SOURCE=$(CSsourceConn -ABNe "select count(*) from $SOURCE_SCHEMA.$SOURCE_TABLE;")
SQL="select count(*),unix_timestamp(min($DATE_COLUMN)),unix_timestamp(max($DATE_COLUMN)),datediff(max($DATE_COLUMN),min($DATE_COLUMN)) from $SOURCE_SCHEMA.$SOURCE_TABLE;"
GET_DB_VALUES=$(CSsourceConn -ABNe "$SQL")


TOTAL_ROWS_SOURCE=$(echo $GET_DB_VALUES| awk '{print $1}' |xargs)
TOTAL_ROWS_TARGET=$(CStargetConn -ABNe "select count(*) from $TARGET_SCHEMA.$TARGET_TABLE;")

echo "Count of rows on source: $TOTAL_ROWS_SOURCE"
echo "Count of rows on target: $TOTAL_ROWS_TARGET"
echo "Script completed succesfully."
