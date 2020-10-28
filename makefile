
packages = java.base/java.util java.base/java.text java.desktop/java.awt.font
add_opens = $(foreach pkg,$(packages),--add-opens $(pkg)=ALL-UNNAMED)

.PHONY: package clean

package:
	@MAVEN_OPTS="$(add_opens)" mvn package

clean:
	@mvn clean
