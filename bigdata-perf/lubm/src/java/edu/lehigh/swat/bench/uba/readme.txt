####################################################
Univ-Bench Artificial Data Generator (UBA)
  Version 1.7
  The Semantic Web and Agent Technologies (SWAT) Lab
  CSE Department, Lehigh University
####################################################

==================
USAGES
==================

command:
   edu.lehigh.swat.bench.uba.Generator
      	[-univ <univ_num>]
	[-index <starting_index>]
	[-seed <seed>]
	[-daml]
	-onto <ontology_url>

options:
   -univ number of universities to generate; 1 by default
   -index starting index of the universities; 0 by default
   -seed seed used for random data generation; 0 by default
   -daml generate DAML+OIL data; OWL data by default
   -onto url of the univ-bench ontology

- The package's path should be on CLASSPATH.

==================
Contact
==================

Yuanbo Guo	yug2@lehigh.edu

For more information about the benchmark, visit its homepage http://www.lehigh.edu/~yug2/Research/SemanticWeb/LUBM/LUBM.htm.
