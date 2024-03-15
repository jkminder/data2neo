from setuptools import find_packages, setup

requirements = [
    'pandas>=1.5.2',
    'ply>=3.11',
    'neo4j>=5.11.0'
]

setup(
    name = "rel2graph", 
    packages = find_packages(),
    version = "1.2.0",
    description = "Library for converting relational data into graph data (neo4j)",
    author = "Julian Minder",
    author_email = "jminder@ethz.ch",   
    url = "github.com/sg-dev/rel2graph",    
    install_requires = requirements,
    python_requires = ">=3.8"
)
  