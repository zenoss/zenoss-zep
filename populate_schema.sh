#!/bin/bash

DBNAME=${DBNAME:-"zenoss_zep"}
DBHOST=${DBHOST:-"localhost"}
DBUSER=${DBUSER:-"root"}
PROMPTPASS=0
FORCE=0

usage() {
    echo "Usage: $(basename "$0") [...]"
    echo ""
    echo "  --dbhost <dbhost> (default '$DBHOST')"
    echo "  --dbname <dbname> (default '$DBNAME')"
    echo "  --dbuser <dbuser> (default '$DBUSER')"
    echo "  --promptpass      (prompts for database password)"
    echo "  --force           (overwrites existing database)"
    echo ""
    exit 0
}

while [ $# -gt 0 ]; do
    ARG=$1
    shift
    case "$ARG" in
        '--dbname')
            DBNAME="$1"
            shift
            ;;
        '--dbhost')
            DBHOST="$1"
            shift
            ;;
        '--dbuser')
            DBUSER="$1"
            shift
            ;;
        '--promptpass')
            PROMPTPASS=1
            ;;
        '--force')
            FORCE=1
            ;;
        *)
            usage
    esac
done

BASEDIR=`dirname "$0"`
BASEDIR=`cd "$BASEDIR" && pwd`

MYSQL="mysql --user=$DBUSER --host=$DBHOST"
if [ $PROMPTPASS -gt 0 ]; then
    MYSQL="$MYSQL --password"
fi


if [ $FORCE -gt 0 ]; then
    DROP_DB="drop database if exists $DBNAME;"
fi

exec $MYSQL <<EOF
$DROP_DB create database $DBNAME;
use $DBNAME;
source $BASEDIR/core/src/main/sql/event_schema.sql
EOF
RC=$?
