#!/bin/bash

#
# Copyright (C) 2010-2011, Zenoss Inc.  All Rights Reserved.
#

BASEDIR=`dirname "$0"`
BASEDIR=`cd "$BASEDIR" && pwd`

SCHEMADIR="$BASEDIR/core/src/main/sql/mysql"
exec "$BASEDIR/dist/src/assembly/bin/zeneventserver-create-db" --schemadir "$SCHEMADIR" "$@"
