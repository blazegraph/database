This directory contains a setup for running BSBM v3 against bigdata.  

Please see [1] and [2] for guidance on setting up and running BSBM against bigdata.

[1] http://www4.wiwiss.fu-berlin.de/bizer/BerlinSPARQLBenchmark
[2] https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=BSBM 

The files in this directory include:

- build.properties - configuration properties for the ant script.

- build.xml - an ant script which may be used to load a generated data set 
              a local bigdata database instance and start a SPARQL 
              end point for that database instance.  You will then run the
              benchmark against that SPARQL end point.

- RWStore.properties - configuration properties for a bigdata database instance
                       suitable for BSBM and backed by the RW persistence engine
                       (single machine read/write bigdata database). This is the
                       recommended database mode for BSBM as it has significantly
                       better performance.
                       
- WORMStore.properties - configuration properties for a bigdata database instance
                         suitable for BSBM and backed by the WORM persistence
                         engine (single machine write once, read many bigdata
                         database).
