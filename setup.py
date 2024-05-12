from setuptools import find_packages, setup

requirements = [
    'pandas>=1.5.2',
    'ply>=3.11',
    'neo4j>=5.11.0'
]

setup(
    name = "data2neo", 
    packages = find_packages(),
    version = "1.4.0",
    description = "Library for converting relational data into graph data (neo4j)",
    author = "Julian Minder",
    author_email = "jminder@ethz.ch",   
    url = "https://github.com/jkminder/data2neo",    
    install_requires = requirements,
    python_requires = ">=3.8"
)
  