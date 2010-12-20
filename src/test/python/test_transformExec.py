import json

import zenoss.zep.transportApi
from zenoss.zep.transformController import TransformController
from mockProtobuf import MockProtobuf

# mock up api classes, to use JSON instead of protobuf
class MockEvent(object):
    __metaclass__ = MockProtobuf
    mockclass = transformApi.Event
    exclusions="count status_change_time first_seen_time last_seen_time action"
EventAdapter.objclass = MockEvent

class MockEventContext(object):
    __metaclass__ = MockProtobuf
    mockclass = transformApi.EventContext
    exclusions = ""
EventContextAdapter.objclass = MockEventContext

# create some event and context data to send to TransformController thru
# mocked JSON interface
eventData = json.dumps({
    "event_class" : "/Unknown",
    "message" : "funny burning smell..."
    })
contextData = json.dumps({
    "count" : 42,
    "first_seen_time" : "2010/1/1",
    "last_seen_time" : "2010/2/2",
    "status_change_time" : "2010/3/3"
    })

# create little method to make it easier to embed test script code within
# nested testing code
def stripIndent(s):
    if s:
        s_lines = s.splitlines()
        leading = len(s_lines[0]) - len(s_lines[0].lstrip())
        s = '\n'.join(line[leading:] for line in s_lines)
    return s
    
# define user-transform code - something like...
script = stripIndent("""\
    if evt.eventClass == "/Unknown":
        if any(word in evt.message.lower() 
                for word in 
                    ("fire", "burning", "smoking", "cinders", "ashes")):
            evt.eventClass = "/Status/ReallyBad/DeviceOnFire"
            if evt.count > 20:
                evt.severity = "CRITICAL"
            else:
                evt.severity = "WARNING"
    """)

# create and run TransformController
controller = TransformController(event_data=eventData, context_data=contextData)
result = controller.runTransformScript(script)

# what did we get back?
print result