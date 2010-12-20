#!/bin/bash

#
# Copyright (c) 2010 Zenoss, Inc. All Rights Reserved.
#

JETTYSTART_JAR=`ls -1 ${JETTY_HOME}/lib/jetty-start*.jar`
PS="ps"

get_pid() {
    if [ -f $PIDFILE ]; then
        local pid=`cat $PIDFILE 2>/dev/null`
        local comm=`$PS -p "$pid" -o comm= 2>/dev/null`
        if [[ "$comm" =~ "$JAVAEXE" ]]; then
            echo $pid
        fi
    fi
}

run() {
    exec java -jar ${JVM_ARGS} ${JETTYSTART_JAR} ${JETTY_ARGS} ${RUN_ARGS}
}

start() {
    local pid=`get_pid`
    if [ -n "$pid" ]; then
        echo is already running
    else
        echo starting...
        java -jar ${JVM_ARGS} ${JETTYSTART_JAR} ${JETTY_ARGS} \
        ${START_ARGS} > /dev/null 2>&1 &
        PID=$!
        disown $PID
        rm -f $PIDFILE
        echo $PID > $PIDFILE
    fi
}

stop() {
    local pid=`get_pid`
    if [ -n "$pid" ]; then
        echo stopping...
        local timeout=120 # 30 seconds
        local has_fp_sleep=
        kill -HUP $pid
        while [ $timeout -gt 0 ]; do
            pid=`get_pid`
            if [ -z "$pid" ]; then
                break
            fi
            if [ -z "$has_fp_sleep" ]; then
                sleep 0.01 > /dev/null 2>&1
                if [ $? -eq 0 ]; then
                    has_fp_sleep="y"
                else
                    has_fp_sleep="n"
                fi
            fi
            if [ "$has_fp_sleep" = "y" ]; then
                sleep 0.25
                timeout=$((timeout-1))
            else
                sleep 1
                timeout=$((timeout-4))
            fi
        done
        if [ -n "$pid" ]; then
            kill -9 "$pid"
        fi
        rm -f $PIDFILE
    else
        echo already stopped
    fi
}

status() {
    local pid=`get_pid`
    if [ -n "$pid" ]; then
        echo program running\; pid=$pid
        exit 0
    else
        echo not running
        exit 100
    fi
}

generic() {
    case "$CMD" in
        start)
            start "$@"
            ;;
        stop)
            stop
            ;;
        restart)
            stop
            start "$@"
            ;;
        status)
            status
            ;;
        debug)
            JVM_ARGS="${JVM_ARGS} -Xdebug -Xrunjdwp:transport=dt_socket,address=8000,server=y,suspend=n"
            run "$@"
            ;;
        run)
            run "$@"
            ;;
        *)
            cat - <<HELP
Usage: $0 {start|stop|restart|status} [options]

  where the commands are:

    start   - start the program

    stop    - stop the program

    restart - stop and then start the program

    status  - Check the status of a daemon.  This will print the current
              process id if it is running.
HELP
            exit 1
    esac
    exit $?
}
