[description]
Configure jetty server side to force all webapps
to use server level logging libraries

[tags]
centralized-webapp-logging

[depends]
resources
webapp
deploy
jul-slf4j
jcl-slf4j
log4j-over-slf4j
logging-logback

[ini]
zenoss-zep-core.version?=2.8.1-SNAPSHOT

[lib]
lib/zep-core-${zenoss-zep-core.version}.jar

[xml]
etc/zeneventserver/jetty/jetty-jul-to-slf4j.xml
etc/zeneventserver/jetty/jetty-mdc-handler.xml
etc/zeneventserver/jetty/jetty-webapp-logging.xml


