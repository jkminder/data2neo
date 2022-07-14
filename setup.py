from setuptools import find_packages, setup

requirements = [
    'py2neo>=2021.2.3',
    'pyodata>=1.7.1',
    'pandas',
    'ply>=3.11'
]

setup(
    name = "rel2graph", 
    packages = find_packages(),
    version = "0.6.1",
    description = "Library for converting relational data into graph data (neo4j)",
    author = "Julian Minder",
    author_email = "jminder@ethz.ch",   
    url = "github.com/sg-dev/rel2graph",    
    install_requires = requirements,
    python_requires = ">=3.7"
)
