from setuptools import setup, find_packages

setup(name='mongoqueue',
      version="0.7.7",
      classifiers=[
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          'Operating System :: OS Independent'],
      author='Kapil Thangavelu',
      author_email='kapil.foss@gmail.com',
      description="A queue using mongo as backend storage.",
      long_description=open("README.rst").read(),
      url='http://github.com/kapilt/mongoqueue',
      license='BSD-derived',
      packages=find_packages(),
      install_requires=["pymongo"],
      setup_requires=["nose", "mongonose"],
      include_package_data=True,
      zip_safe=True,
      )
