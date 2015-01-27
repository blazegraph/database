This directory contains a setup for some Gather Apply Scatter (GAS) based
graph mining algorithms against bigdata.  

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
