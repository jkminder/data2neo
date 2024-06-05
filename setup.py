from setuptools import find_packages, setup

requirements = [
    'pandas>=1.5.2',
    'ply>=3.11',
    'neo4j>=5.11.0'
]

long_desc = open('README.md').read()
# replace local links
long_desc = long_desc.replace('docs/source/assets/images/data2neo_banner.png', 'https://github.com/jkminder/data2neo/blob/main/docs/source/assets/images/data2neo_banner.png?raw=true')

setup(
    name = "data2neo", 
    packages = find_packages(),
    version = "1.4.3",
    description = "Library for converting relational data into graph data (neo4j)",
    author = "Julian Minder",
    author_email = "jminder@ethz.ch",   
    url = "https://github.com/jkminder/data2neo",    
    install_requires = requirements,
    python_requires = ">=3.8",
    long_description=long_desc,
    long_description_content_type='text/markdown'
)