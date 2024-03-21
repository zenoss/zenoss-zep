#
# Jetty Servlet Module
#

[depend]
server

[ini]
jetty-jakarta-servlet-api.version?=5.0.2

[lib]
lib/jetty-servlet-${jetty.version}.jar
lib/jetty-jakarta-servlet-api-${jetty-jakarta-servlet-api.version}.jar