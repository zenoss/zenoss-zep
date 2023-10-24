[description]
Configures Jetty logging to use Logback Logging.
SLF4J is used as the core logging mechanism.

[tags]
logging

[depends]
slf4j
resources

[provides]
logging

[lib]
lib/logback-classic-${logback.version}.jar
lib/logback-core-${logback.version}.jar
lib/janino-3.1.8.jar
lib/commons-compiler-3.1.8.jar

[ini]
logback.version?=1.4.1
jetty.webapp.addServerClasses+=,ch.qos.logback.

[license]
Logback: the reliable, generic, fast and flexible logging framework.
Copyright (C) 1999-2012, QOS.ch. All rights reserved.

This program and the accompanying materials are dual-licensed under
either:

the terms of the Eclipse Public License v1.0
as published by the Eclipse Foundation:
http://www.eclipse.org/legal/epl-v10.html

or (per the licensee's choosing) under

the terms of the GNU Lesser General Public License version 2.1
as published by the Free Software Foundation:
http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html