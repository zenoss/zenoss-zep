from zenoss.zep.propertyMonitor import PropertyMonitor

class Test(object):
    __metaclass__ = PropertyMonitor
    fields = "X Y Z"
    readonly_fields = "A B C"
    compatibility_map = { "yy" : "Y", "aa" : "A"}

# create test object, initialize one of the attributes using 
# named constructor arg - this should not be recorded as a change
t2 = Test(Z=50)
print t2.changed, "should be False"

# list all attributes - should include attributes created by metaclass
print dir(t2)

# initialize another attribute, and mark object
t2.Y = "blah"
t2.mark()
print "init complete"

# set Z to same as init value, should not be recorded as a change
t2.Z = 50
print t2.Z
print t2.changed, "should be False"

# use += to get and set Z - this should be recorded as a change
t2.Z += 1
print t2.Z
print t2.changed, "should be True"
print t2.get_changes()

# clear change log
t2.mark()

# set another property - this should be recorded as a change
t2.X = 100
print t2.changed, "should be True"

# set a property thru an alias - this should be recorded as a change on
# the new field
t2.yy = "Woot!"
print t2.get_changes()

t2.A = "assign to readonly property"
t2.freeze()
t2.A = "this value will be silently ignored"
print "'%s' should be 'assign to readonly property'" % t2.A
print "'%s' should be 'assign to readonly property'" % t2.aa
