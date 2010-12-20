from setuptools import setup, find_packages

setup(
   name = "zenoss.zep",
   version = "0.1",
   packages = find_packages(),
   long_description=open('README.txt').read(),
   install_requires = ['setuptools'],
   include_package_data=True,
   package_data={
        # 'zenoss/protocols/twisted': ['zenoss/protocols/twisted/amqp0-9-1.xml'],
   },

   # in the zenoss namespace
   namespace_packages = ['zenoss'],
   # metadata for upload to PyPI
   author = "Zenoss",
   author_email = "support@zenoss.com",
   description = "Zenoss Event Processing integration component.",
   license = "GPLv2",
   keywords = "zenoss zep transform",
   url = "http://www.zenoss.com/",
   zip_safe=False,
   # cmdclass = {
   #      'build_proto' : build_proto,
   #      'sdist' : protoc_sdist,
   #      }
)

