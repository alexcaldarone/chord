from setuptools import setup, find_packages

setup(name="Chord Protocol simulator",
      version = "0.0",
      author = "Alex John Caldarone",
      description = "Simulator for Chord protocol",
      license = "MIT", 
      packages=find_packages(),
      install_requires = [
          "numpy",
          "pandas"
      ],
      install_data = False
      )