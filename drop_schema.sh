#!/bin/bash

##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2012, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


BASEDIR=`dirname "$0"`
BASEDIR=`cd "$BASEDIR" && pwd`

DB_TYPE=
DB_HOST=
DB_PORT=
DB_NAME=
DB_ADMIN_USERNAME=
DB_ADMIN_PASSWORD=${DB_ADMIN_PASSWORD:-""}

while [ $# -gt 0 ]; do
    arg="$1"
    shift
    if [ $# -eq 0 ]; then
        echo "Missing argument for $arg" >&2
        exit 1
    fi
    val="$1"
    shift
    case "$arg" in
        --dbtype)
            DB_TYPE="$val"
            ;;
        --dbhost)
            DB_HOST="$val"
            ;;
        --dbport)
            DB_PORT="$val"
            ;;
        --dbname)
            DB_NAME="$val"
            ;;
        --dbadminuser)
            DB_ADMIN_USERNAME="$val"
            ;;
        --dbadminpass)
            DB_ADMIN_PASSWORD="$val"
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            exit 1
            ;;
    esac
done

if [ -z "$DB_TYPE" ]; then
    echo "Required parameter --dbtype not specified" >&2 && exit 1
fi
if [ -z "$DB_HOST" ]; then
    echo "Required parameter --dbhost not specified" >&2 && exit 1
fi
if [ -z "$DB_PORT" ]; then
    echo "Required parameter --dbport not specified" >&2 && exit 1
fi
if [ -z "$DB_NAME" ]; then
    echo "Required parameter --dbname not specified" >&2 && exit 1
fi
if [ -z "$DB_ADMIN_USERNAME" ]; then
    echo "Required parameter --dbadminuser not specified" >&2 && exit 1
fi
# Don't need to validate DB_ADMIN_PASSWORD - it can be empty.

rc=0
case "$DB_TYPE" in
    mysql)
        mysqladmin -f --host="$DB_HOST" --port="$DB_PORT" \
                   --user="$DB_ADMIN_USERNAME" \
                   --password="$DB_ADMIN_PASSWORD" \
                   drop "$DB_NAME"
        rc=$?
        ;;
    postgresql)
        TMPDIR=${TMPDIR:-"/tmp"}
        passfile=`mktemp ${TMPDIR}/pg.XXXXXX` || exit 1
        trap "rm -f '$passfile'" EXIT
        export PGPASSFILE="$passfile"
        echo "*:*:*:*:${DB_ADMIN_PASSWORD}" >> $passfile
        dropdb --host="$DB_HOST" --port="$DB_PORT" \
               --username="$DB_ADMIN_USERNAME" -w "$DB_NAME"
        rc=$?
        rc=$?
        ;;
    *)
        echo "Unknown database type: $DB_TYPE" >&2
        exit 1
        ;;
esac

exit $rc
