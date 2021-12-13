from setuptools import find_packages, setup

setup(
    name = "rel2graph", 
    packages = find_packages(include=["rel2graph"]),
    version = "0.1.0",
    description = "Library for converting relational data into graph data (neo4j)",
    author = "Julian Minder"
)