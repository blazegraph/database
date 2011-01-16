This directory contains a setup for running BSBM v3 against bigdata.  The main
files are:

- bsbmtools - the bsbm3 source distribution.

- build.properties - configuration properties for the ant script.

- build.xml - an ant script which may be used to generate a BSBM data set, load
              the data set into a bigdata database instance, start a SPARQL 
              end point for that database instance, and run the BSBM benchmark
              against that SPARQL end point.

- RWStore.properties - configuration properties for a bigdata database instance
                       suitable for BSBM and backed by the RW persistence engine
                       (single machine read/write bigdata database). This is the
                       recommended database mode for BSBM as it has significantly
                       better performance.
                       
- WORMStore.properties - configuration properties for a bigdata database instance
                         suitable for BSBM and backed by the WORM persistence
                         engine (single machine write once, read many bigdata
                         database).

Other requirements include:

- A 64-bit OS and a 64-bit server JVM.  We have tested most extensible with Oracle
  JDK 1.6.0_17.

- Apache ant (version 1.8.0+).
  
- Bigdata (check it out from SVN).
                         
To get started:

1. Edit bigdata-perf/bsbm3/build.properties.

1. In the top-level directory of the bigdata source tree, review build.properties
   and then do:
   
   a. "ant bundleJar".
   
   Note: You will need to rerun this ant target any time you update the code
   from SVN or if you make edits to the source tree.
   
2. Change to the bigdata-perf/bsbm3 directory:

   a. "ant run-generator" (generates the BSBM data set).
   
   b. "ant run-load" (loads the generated data set into a bigdata instance).
   
   c. "ant start-nano-server" (starts the SPARQL end point).
   
   d. "ant run-query" (runs the benchmark).

There are a variety of other ant tasks in that directory which may be used to
run load and run the BSBM qualification data set, etc. 
