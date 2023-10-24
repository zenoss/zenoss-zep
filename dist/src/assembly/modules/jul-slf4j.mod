[description]
Provides a Java Util Logging binding to SLF4J logging.

[tags]
logging
slf4j
internal

[depends]
slf4j

[provides]
jul-api
jul-impl
slf4j+jul

[lib]
lib/jul-to-slf4j-${slf4j.version}.jar

[exec]
-Djava.util.logging.config.file?=${jetty.base}/etc/zeneventserver/jetty/java-util-logging.properties