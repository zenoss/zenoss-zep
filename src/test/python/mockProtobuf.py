import json

# metaclass to mock communication using json instead of protobuf
class MockProtobuf(type):
    # Testing metaclass to support internal testing using JSON instead of
    # using protobuf classes
    def __init__(cls, name, bases, attrs):
        if "mockclass" in attrs:
            # get fields and readonly_fields from class being mocked
            mock = attrs["mockclass"]
            fields = mock.fields.split()
            readonly_fields = mock.readonly_fields.split()
        else:
            fields = attrs["fields"].split()
            readonly_fields = attrs["readonly_fields"].split()
            
        if "exclusions" in attrs:
            exclusions = attrs["exclusions"].split()
            fields = [f for f in fields if f not in exclusions]
            readonly_fields = [f for f in readonly_fields if f not in exclusions]

        # wrap __init__ initializer
        cls.wrap_init(fields + readonly_fields)
        
        # add fauxPB interface (using JSON for serialization)
        cls.addProtobufSerializationMethods()
        
    def wrap_init(cls, attribs):
        # wrap __init__ with additional initialization code:
        # - define 'xxx_' attributes for each 'xxx' field, and initialize to None
        # - initialize any attributes given in **kwargs
        # - add '_changed' dict for tracking changes
        setattr(cls, '__orig__init__', getattr(cls, '__init__', None))
        def __new_init__(self, *args, **kwargs):
            if self.__orig__init__ is not None:
                self.__orig__init__(*args)

            # initialize defined attribs
            self.__dict__.update(dict((f,None) for f in attribs))
                
            # update attribs
            self.__dict__.update(dict((k,v) for k,v in kwargs.items() 
                                                if k in attribs))

        setattr(cls, '__init__', __new_init__)
    
    def addProtobufSerializationMethods(cls):
        # impersonate protobuf serialization methods with methods that
        # return JSON strings
        def ParseFromString_fn(self, s):
            self.__dict__.update(json.loads(s))
        def SerializeToString_fn(self):
            name = self.__class__.__name__
            if name.startswith("Mock"):
                name = name[4:]
            return '"%s" : %s' % (name, json.dumps(self.__dict__, indent="  "))
        setattr(cls, 'ParseFromString', ParseFromString_fn)
        setattr(cls, 'SerializeToString', SerializeToString_fn)

