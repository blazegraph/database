
# What to do to implement A*star algorithm #

## I. PLan Stage 2

### 1) Given
   - we have working (in generally) ASTAR algorythm. 
   
   - need to put everything in order and 
   
   -make next steps
 
### 2) Put in order statement info 
   (obj pred subj file) and parsing
   
### 3) write filtering

### 4) pass external data (- filter use, filter ignore)

### 5) use python client
       to improve repeatability
 
### 6) store everything in git


## II. PLan stage1

### 1) understand implementation (done) ### 

  1-1) understand class relations around BFS/ SSSP/ GAS*

  1-2) understand how to implement new search

  1-3) understand how to switch between different searches

  1-4) try debugger ?

  1-5) work with docs/ papers

### 2) prepare test data (done)

  2-1) run blasegraph, import small data set (RDF)

  2-2) watch how does it work and get results

### 3) learn how to build (done)

  3-1) build original

  3-2) add new class and build modified
  
### 4) make modifications (done)

  4-1) make new searchalgorythm

  4-2) debug

  4-3) build new version

### 5) use Java 1_8 (done)
 and watch for conflicts fo different version - for example env JAVA_HOME
 - see readme.md


## III.Notes

### 1) Java
  Was successfull with Java 1.8.0_342. Attempts to use Java 11 failed.
  Also - need to install full SDK  Only Java is not enough - also need Javac and so on.
  Otherwise Maven will not build.

1-1)use UPDATE-JA

### 2) after build- results re in bigdata.jar/target

   run it like: 

```
   > java -server -Xmx4g -jar blazegraph.jar 
```
   
   but with different jar file: 

```
   > java -server -Xmx4g -jar /bigdata-jar/target/bigdata-jar-2.1.6-master-20220923.jar
```
    
### 3) load :	

```
load <file:///home/gserdyuk/epddp/blazegraph-wk/data/data1.n3>
```

   query:	

### 4) load :	

```
load <file:///home/gserdyuk/epddp/database-astr/mods-astar/data/stack-ov-1.ttl>

query:	select * where {<http://prism.uvsq.fr/c> ?p ?s}
```
   
   or Load :

```
	load <file:///home/gserdyuk/epddp/database-astr/mods-astar/data/stack-ov-1.ttl>

query:    
    PREFIX prism: <http://prism.uvsq.fr/>
	select * where {prism:c ?p ?s}
```
   or

```
clear all
```

### 5) GAS things:  (search with with stack-ov-1)
```
  PREFIX prism: <http://prism.uvsq.fr/>
  PREFIX gas: <http://www.bigdata.com/rdf/gas#>
  SELECT ?depth ?predecessor ?linkType ?out {
  SERVICE gas:service {
     gas:program gas:gasClass "com.bigdata.rdf.graph.analytics.BFS" .
     gas:program gas:in prism:c . # one or more times, specifies the initial frontier.
     gas:program gas:target prism:d . # only retain vertices along paths to these target vertices.
     gas:program gas:target prism: . # only retain vertices along paths to these target vertices.
     gas:program gas:out ?out . # exactly once - will be bound to the visited vertices.
     gas:program gas:out1 ?depth . # exactly once - will be bound to the depth of the visited vertices.
     gas:program gas:out2 ?predecessor . # exactly once - will be bound to the predecessor.
     gas:program gas:maxIterations 4 . # optional limit on breadth first expansion.
     gas:program gas:maxVisited 2000 . # optional limit on the #of visited vertices.
  }
  ?predecessor ?linkType ?out . # figure out what link type(s) connect a vertex with a predecessor
} 
order by desc(?depth)
limit 100
```

### 6) more GAS things:  (search with with stack-ov-1)

```
# use with stack-ov-1.ttl
PREFIX prism: <http://prism.uvsq.fr/>
  PREFIX gas: <http://www.bigdata.com/rdf/gas#>
  SELECT ?depth ?predecessor ?linkType ?out {
  SERVICE gas:service {
     gas:program gas:gasClass "com.bigdata.rdf.graph.analytics.ASTAR" .
     gas:program gas:in prism:a . # one or more times, specifies the initial frontier.
     gas:program gas:target prism:m . # only retain vertices along paths to these target vertices.
     gas:program gas:out ?out . # exactly once - will be bound to the visited vertices.
     gas:program gas:out1 ?depth . # exactly once - will be bound to the depth of the visited vertices.
     gas:program gas:out2 ?predecessor . # exactly once - will be bound to the predecessor.
     gas:program gas:maxIterations 6 . # optional limit on breadth first expansion.
     gas:program gas:maxVisited 2000 . # optional limit on the #of visited vertices.
  }
  ?predecessor ?linkType ?out . # figure out what link type(s) connect a vertex with a predecessor
} 
order by desc(?depth)
limit 100
```

## X. Useful links

https://techpiezo.com/linux/change-default-java-version-in-ubuntu-22-04/

https://askubuntu.com/questions/740757/switch-between-multiple-java-versions
https://github.com/blazegraph/database/wiki/Quick_Start

https://linuxhint.com/install_jdk_14_ubuntu/ - no

https://cwiki.apache.org/confluence/display/MAVEN/DependencyResolutionException

https://habr.com/ru/post/421323/
https://habr.com/ru/post/343550/
https://habr.com/ru/post/77333/

https://github.com/blazegraph/database

https://www.cs.rochester.edu/u/nelson/courses/csc_173/graphs/sssp.html

https://stackoverflow.com/questions/28900290/querying-a-graph-path-in-sparql

## XI. useful commands

```
load file: load <file:///home/gserdyuk/epddp/blazegraph-samples/sample-sesame-embedded/src/main/resources/data.n3>
```
```
select: select * where {<http://blasegraph.com/blazegraph> ?p ?o}
```

```
load <file:///home/gserdyuk/epddp/blazegraph-wk/data/kitchen.n3>
```
