[description]
Provides a Log4j bridge to SLF4J logging.

[tags]
logging
slf4j
internal

[depends]
slf4j

[provides]
log4j-api
log4j-impl
slf4j+log4j

[lib]
lib/log4j-over-slf4j-${slf4j.version}.jar