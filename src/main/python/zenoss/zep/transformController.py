import zenoss.zep.transformApi
import zenoss.protocols.protobufs.zep_pb2 as zep_protobufs
import zenoss.protocols.protobufs.model_pb2 as model_protobufs

class TransformObjectAdapter(object):
    """Class for adapting protobuf objects to/from transform proxies
       with change-monitoring properties."""
    def __init__(self):
        self.object = self.objclass()
        self.userproxy = self.proxyclass()

    def initialize(self, initdata):
        # get object definition data
        self.object.ParseFromString(initdata)
        
        # copy values to user proxy object, which will track mods made in transform
        for field in (self.proxyclass.fields.split() + 
                        self.proxyclass.readonly_fields.split()):
            if field not in self.objclass.exclusions.split():
                setattr(self.userproxy, field, getattr(self.object,field))
            
        # mark proxy object as initialized - all changes from here forward will be tracked
        self.userproxy.mark()

    @property
    def proxy(self):
        return self.userproxy

    @property
    def result(self):
        if self.userproxy.changed:
            for (field,value) in self.userproxy.get_changes().items():
                setattr(self.object, field, value)
        return self.object.SerializeToString()


# define adapter classes for mapping serialized objects to proxy objects to
# be passed to the transform script
class EventAdapter(TransformObjectAdapter):
    proxyclass = transformApi.Event
    objclass = zep_protobufs.Event

class EventContextAdapter(TransformObjectAdapter):
    proxyclass = transformApi.EventContext
    objclass = zep_protobufs.EventContext

class DeviceAdapter(TransformObjectAdapter):
    proxyclass = transformApi.Device
    objclass = model_protobufs.Device

class ComponentAdapter(TransformObjectAdapter):
    proxyclass = transformApi.Component
    objclass = model_protobufs.Component

class ServiceAdapter(TransformObjectAdapter):
    proxyclass = transformApi.Service
    objclass = model_protobufs.Service


class TransformController(object):
    """Class for running transforms. Initialize with named arguments, each
       one containing the serialized contents of the respective object:
        - event_data <- Event
        - context_data <- EventSummary
        - etc.
    """
    def __init__(self, **kwargs):
        # copy named arguments to attributes in self
        self.__dict__.update(kwargs)

    def runTransformScript(self, script_body):
        """Run transform script in the context of the event transform
           objects used to initialize this controller.  The script content
           itself is passed in as a string.  The script is run, and the
           modified Event is returned as a serialized string.
        """
        # create and initialize adapters
        eventAdapter = EventAdapter()
        contextAdapter = EventContextAdapter()
        deviceAdapter = DeviceAdapter()
        componentAdapter = ComponentAdapter()
        serviceAdapter = ServiceAdapter()

        # for any initialization data that was sent, initialize
        # corresponding adapter
        for name in "event context device component service".split():
            fullattrname = attr + "_data"
            fulladaptername = attr + "Adapter"
            adapter = getattr(self, fulladaptername)
            if hasattr(self, fullattrname):
                adapter_data = getattr(self, fullattrname)
                adapter.initialize(adapter_data)

        # TODO - decide whether to do this here or in Java (probably in Java, before initializing this object)
        # TODO - copy event summary info to event from context.summary
        # TODO - copy acknowledged_by_user_uuid cleared_by_event_uuid to event from 
        #     context.summary
        # TODO - copy device info from device to event

        # run provided script        
        try:
            exec script_body in { 
                "evt" : eventAdapter.proxy, 
                "ctx" : contextAdapter.proxy,
                "dev" : deviceAdapter.proxy,
                "device" : deviceAdapter.proxy,
                "component" : componentAdapter.proxy,
                "service" : serviceAdapter.proxy,
            }
        except Exception, e:
            print e
            return None

        else:
            # now get back the updated Event
            return eventAdapter.result
            
        finally:
            del eventAdapter
            del contextAdapter
