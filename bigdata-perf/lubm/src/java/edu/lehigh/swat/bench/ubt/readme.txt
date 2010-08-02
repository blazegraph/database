####################################################
Univ-Bench Tester (UBT)
  Version 1.1
  The Semantic Web and Agent Technologies (SWAT) Lab
  CSE Department, Lehigh University
####################################################

==================
FILES
==================

config.kb.example.dldb - example knowledge base configuration file for DLDB
config.query.example.dldb - example query file for DLDB (in KIF-like language)
config.kb.example.sesame - example knowledge base configuration file for Sesame
config.query.example.sesame - example query file for Sesame (in RQL)
class files inside package edu.lehigh.swat.bench.ubt
source files inside package edu.lehigh.swat.bench.ubt
readme.txt - this file

==================
REQUIREMENTS
==================

- Setup of the target repositories including creating their wrappers to instantiate the repository API is required prior to the test.
- Java option -Xmx256m or above is recommended. Otherwise SQL Out of Memory error may occur.

==================
USAGES
==================

   java edu.lehigh.swat.bench.ubt.Test load <kb_config_file>
or java edu.lehigh.swat.bench.ubt.Test query <kb_config_file> <query_file>
or java edu.lehigh.swat.bench.ubt.Test memory <kb_config_file> <query_file>

load: specified for data loading test
query: specified for query test
memory: specified for memory-based system; query test is done immediately after loading

- Make sure the path of this package and the pathes of the class/library of all the target repositories are on CLASSPATH.
- Use <kb_config_file> to specify the target knowledge base systems and <query_file> to describe the test queries.
- Specify the name of your classes which implement edu.lehigh.swat.bench.ubt.api.RepositoryFactory using "class" items in file <kb_config_file>.

==================
Contact
==================

Yuanbo Guo	yug2@lehigh.edu

For more information about the benchmark, visit its homepage http://www.lehigh.edu/~yug2/Research/SemanticWeb/LUBM/LUBM.htm.