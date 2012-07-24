#!/bin/bash

##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


BASEDIR=`dirname "$0"`
BASEDIR=`cd "$BASEDIR" && pwd`

SCHEMADIR="$BASEDIR/core/src/main/sql"
exec "$BASEDIR/dist/src/assembly/bin/zeneventserver-create-db" --schemadir "$SCHEMADIR" "$@"
