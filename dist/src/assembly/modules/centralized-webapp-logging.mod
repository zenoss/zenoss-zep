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

[xml]
etc/zeneventserver/jetty/jetty-jul-to-slf4j.xml


