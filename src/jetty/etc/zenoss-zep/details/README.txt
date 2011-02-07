This directory contains properties files which define which event details
should be indexed by ZEP and made searchable/sortable from the event console
or REST clients.

The property file syntax is:

    <prefix>.key=<detail_name>
    <prefix>.type=<STRING/INTEGER>
    <prefix>.display_name=<Display Name>

For example:

    priority.key=zenoss.device.priority
    priority.type=INTEGER
    priority.display_name=Priority

This indicates to ZEP that if an event has a detail named
'zenoss.device.priority', it should have its value indexed as an INTEGER. Multiple
details can be specified in each file.
