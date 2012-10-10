#!/bin/bash

##############################################################################
# 
# Copyright (C) Zenoss, Inc. 2010-2011, all rights reserved.
# 
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
# 
##############################################################################


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
    PID=$$
    rm -f $PIDFILE
    echo $PID > $PIDFILE
    exec java ${JVM_ARGS} -jar ${JETTYSTART_JAR} ${JETTY_ARGS} ${RUN_ARGS}
}

# Waits for the process to be started (assumes when the ZEP port is listening the
# application has started up completely).
wait_for_startup() {
    local port=$1
    local timeout=${ZENOSS_STARTUP_TIMEOUT}
    local elapsed=0
    echo -n "Waiting for zeneventserver to start"
    while [ "${elapsed}" -lt "${timeout}" ]; do
        netstat -lant | awk '$6 ~ /LISTEN/ { print $4 }' | grep "[.:]${port}$" > /dev/null
        if [ $? -eq 0 ]; then
            break
        fi
        sleep 1
        echo -n "."
        elapsed=$((${elapsed}+1))
    done
    echo ""
    if [ ${timeout} -eq ${elapsed} ]; then
        echo "zeneventserver failed to start within ${timeout} seconds." >&2
        echo "Please check the following log file for more details: " >&2
        echo "  ${ZENHOME}/log/zeneventserver.log" >&2
        return 1
    fi
    return 0
}

start() {
    local port=$1
    local pid=`get_pid`
    if [ -n "$pid" ]; then
        echo is already running
    else
        echo starting...
        JVM_ARGS="$JVM_ARGS -DZENOSS_DAEMON=y"
        # Redirect stdout/stderr to separate log file
        JETTY_ARGS="$JETTY_ARGS --pre=etc/zeneventserver/jetty/jetty-logging.xml"
        java ${JVM_ARGS} -jar ${JETTYSTART_JAR} ${JETTY_ARGS} \
        ${START_ARGS} > /dev/null 2>&1 &
        PID=$!
        disown $PID
        rm -f $PIDFILE
        echo $PID > $PIDFILE
        wait_for_startup ${port}
    fi
}

stop() {
    local pid=`get_pid`
    if [ -n "$pid" ]; then
        echo stopping...
        local timeout=$((${ZENOSS_STARTUP_TIMEOUT}*4))
        local has_fp_sleep=
        kill -TERM $pid
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
            kill -9 "$pid" || return 1
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
        exit 3
    fi
}

audit() {
    case "${CMD}" in
      status)
        ;;
      help)
        ;;
      *)
        if [ -e $ZENHOME/bin/zensendaudit ] ; then
          zensendaudit kind=Daemon action=$CMD daemon=zeneventserver > /dev/null 2>&1
        fi
        ;;
    esac
}

generic() {
    CMD=$1
    shift
    audit

    ZEP_PORT=8084
    while getopts "p:v:" flag
    do
        case "$flag" in
            p)
                case "$OPTARG" in
                    [1-9][0-9]*)
                        ZEP_PORT=${OPTARG}
                        ;;
                    *)
                        echo "Invalid argument for $flag: $OPTARG" >&2
                        exit 1
                        ;;
                esac
                JVM_ARGS="${JVM_ARGS} -Djetty.port=${ZEP_PORT}"
                ;;
            v)
                OPTARG=`echo "$OPTARG" | tr "[:lower:]" "[:upper:]"`
                case "$OPTARG" in
                    TRACE)
                        ZENOSS_LOG_LEVEL="TRACE"
                        ;;
                    10|DEBUG)
                        ZENOSS_LOG_LEVEL="DEBUG"
                        ;;
                    20|INFO)
                        ZENOSS_LOG_LEVEL="INFO"
                        ;;
                    30|WARN|WARNING)
                        ZENOSS_LOG_LEVEL="WARN"
                        ;;
                    40|ERROR)
                        ZENOSS_LOG_LEVEL="ERROR"
                        ;;
                    *)
                        echo "Invalid log level: '$OPTARG'" >&2
                        exit 1
                        ;;
                esac
                JVM_ARGS="$JVM_ARGS -DZENOSS_LOG_LEVEL=$ZENOSS_LOG_LEVEL"
                ;;
            *)
                echo "Invalid argument: $flag" >&2
                exit 1
        esac
    done

    case "$CMD" in
        start)
            start ${ZEP_PORT}
            ;;
        stop)
            stop
            ;;
        restart)
            stop
            start ${ZEP_PORT}
            ;;
        status)
            status
            ;;
        debug)
            JVM_ARGS="${JVM_ARGS} -Xdebug -Xrunjdwp:transport=dt_socket,address=8000,server=y,suspend=y"
            run "$@"
            ;;
        profile)
            yjp_agent=""
            case `uname -s` in
                Linux)
                    case `uname -i` in
                        x86_64)
                            yjp_agent="linux-x86-64/libyjpagent.so"
                            ;;
                        i[3456]86)
                            yjp_agent="linux-x86-32/libyjpagent.so"
                            ;;
                    esac
                    ;;
                Darwin)
                    yjp_agent="mac/libyjpagent.jnilib"
                    ;;
            esac
            if [ -z "$yjp_agent" ]; then
                echo "Unknown hardware type: `uname -s` (`uname -i`)" >&2
                exit 1
            fi
            yjp_bin=`which yjp.sh`
            if [ -z "$yjp_bin" ]; then
                echo "Failed to find yjp.sh in PATH" >&2
                exit 1
            fi
            yjp_home=`dirname "$yjp_bin"`
            JVM_ARGS="${JVM_ARGS} -agentpath:$yjp_home/$yjp_agent=sampling,disablealloc"
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
