#============================================================================
#
# Copyright (C) Zenoss, Inc. 2013, all rights reserved.
#
# This content is made available according to terms specified in
# License.zenoss under the directory where your Zenoss product is installed.
#
#============================================================================
.DEFAULT_GOAL   := help # all|build|clean|distclean|devinstall|install|help

#============================================================================
# Build component configuration.
#
# Beware of trailing spaces.
# Don't let your editor turn tabs into spaces or vice versa.
#============================================================================
REQUIRES_JDK          = 1
srcdir                = core dist webapp

# ZEP has several child projects, each of which contributes to this build
COMPONENT_WEBAPP      = zep-webapp
COMPONENT_DIST        = zep-dist
#
# For zapp components, keep blddir aligned with src/main/assembly/zapp.xml
#
blddir                = target

#============================================================================
# Hide common build macros, idioms, and default rules in a separate file.
#============================================================================

#---------------------------------------------------------------------------#
# Pull in zenmagic.mk
#---------------------------------------------------------------------------#
# Locate and include common build idioms tucked away in 'zenmagic.mk'
# This holds convenience macros and default target implementations.
#
# Generate a list of directories starting here and going up the tree where we
# should look for an instance of zenmagic.mk to include.
#
#     ./zenmagic.mk ../zenmagic.mk ../../zenmagic.mk ../../../zenmagic.mk
#---------------------------------------------------------------------------#
NEAREST_ZENMAGIC_MK := $(word 1,$(wildcard ./zenmagic.mk $(shell for slash in $$(echo $(abspath .) | sed -e "s|.*\(/obj/\)\(.*\)|\1\2|g" -e "s|.*\(/src/\)\(.*\)|\1\2|g" | sed -e "s|[^/]||g" -e "s|/|/ |g"); do string=$${string}../;echo $${string}zenmagic.mk; done | xargs echo)))

ifeq "$(NEAREST_ZENMAGIC_MK)" ""
    $(warning "Missing zenmagic.mk needed by the $(COMPONENT)-component makefile.")
    $(warning "Unable to find our file of build idioms in the current or parent directories.")
    $(error   "A fully populated src tree usually resolves that.")
else
    #ifneq "$(MAKECMDGOALS)" ""
    #    $(warning "Including $(NEAREST_ZENMAGIC_MK) $(MAKECMDGOALS)")
    #endif
    include $(NEAREST_ZENMAGIC_MK)
endif

# List of source files needed to build this component.
COMPONENT_SRC ?= $(DFLT_COMPONENT_SRC)
COMPONENT_VERSION ?= $(DFLT_COMPONENT_VERSION)

# Name of jar we're building: my-component-x.y.z.jar
COMPONENT_JAR ?= "$(COMPONENT_WEBAPP)-$(COMPONENT_VERSION).war"

# Specify install-related directories to create as part of the install target.
INSTALL_MKDIRS = $(_DESTDIR)$(prefix) $(_DESTDIR)$(prefix)/log $(_DESTDIR)$(prefix)/bin $(_DESTDIR)$(prefix)/etc/zeneventserver $(_DESTDIR)$(prefix)/share $(_DESTDIR)$(prefix)/var/run $(_DESTDIR)$(prefix)/webapps

ifeq "$(COMPONENT_JAR)" ""
    $(call echol,"Please investigate the COMPONENT_JAR macro assignment.")
    $(error Unable to derive component jar filename from pom.xml)
else
    # Name of binary tar we're building: my-component-x.y.z.tar.gz
    COMPONENT_TAR = $(COMPONENT_DIST)-$(COMPONENT_VERSION).tar.gz
endif
TARGET_JAR := webapp/$(blddir)/$(COMPONENT_JAR)
TARGET_TAR := dist/$(blddir)/$(COMPONENT_TAR)

#============================================================================
# Subset of standard build targets our makefiles should implement.  
#
# See: http://www.gnu.org/prep/standards/html_node/Standard-Targets.html#Standard-Targets
#============================================================================
.PHONY: all build clean devinstall distclean install help mrclean uninstall
all build: $(TARGET_TAR)

# Targets to build the binary *.tar.gz.
ifeq "$(_TRUST_MVN_REBUILD)" "yes"
$(TARGET_TAR): checkenv
else
$(TARGET_TAR): $(CHECKED_ENV) $(COMPONENT_SRC)
endif
	$(call cmd,MVN,package,$@)
	@$(call echol,$(LINE))
	@$(call echol,"$(_COMPONENT) built.  See $@")

$(INSTALL_MKDIRS):
	$(call cmd,MKDIR,$@)

# NB: Use the "|" to indicate an existence-only dep rather than a modtime dep.
#     This rule should not trigger rebuilding of the component we're installing.
install: | $(INSTALL_MKDIRS) 
	@if [ ! -f "$(TARGET_TAR)" ];then \
        $(call echol) ;\
		$(call echol,"Error: Missing $(TARGET_TAR)") ;\
		$(call echol,"Unable to $@ $(_COMPONENT).") ;\
		$(call echol,"$(LINE)") ;\
		$(call echol,"Please run 'make build $@'") ;\
		$(call echol,"$(LINE)") ;\
		exit 1 ;\
	fi 
	$(call cmd,UNTAR,$(abspath $(TARGET_TAR)),$(_DESTDIR)$(prefix))
	@$(call echol,$(LINE))
	@$(call echol,"$(_COMPONENT) installed to $(_DESTDIR)$(prefix)")

devinstall: dev% : %
	@$(call echol,"Add logic to the $@ rule if you want it to behave differently than the $< rule.")

uninstall:
	@$(call echol,"Uninstall isn't supported for ZEP.")

clean: dflt_component_clean

mrclean distclean: dflt_component_distclean

help: dflt_component_help
