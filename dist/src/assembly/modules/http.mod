#
# Jetty HTTP Connector
#

[depend]
server

[xml]
etc/zeneventserver/jetty/jetty-http.xml

[ini-template]
### HTTP Connector Configuration

## HTTP port to listen on
jetty.port=8084

## HTTP idle timeout in milliseconds
http.timeout=300000

## HTTP Socket.soLingerTime in seconds. (-1 to disable)
# http.soLingerTime=-1

## Parameters to control the number and priority of acceptors and selectors
# http.acceptors=1
# http.selectors=1
# http.selectorPriorityDelta=0
# http.acceptorPriorityDelta=0
