[tags]
logging

[provides]
slf4j

[lib]
lib/slf4j-api-${slf4j.version}.jar

[ini]
slf4j.version?=2.0.1
jetty.webapp.addServerClasses+=,org.slf4j.