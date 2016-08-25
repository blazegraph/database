#Blazegraph Vocabularies
Blazegraph Custom Vocabularies and Inline URI factories are a great way to get the best load and query performance from Blazegraph.   This package contains some commonly used vocabularies for public data sets.

##Pubchem
The [Pubchem](https://pubchem.ncbi.nlm.nih.gov/rdf/) data set is widely used in industry and research. 

_PubChemRDF content includes a number of semantic relationships, such as those between compounds and substances, the chemical descriptors associated with compounds and substances, the relationships between compounds, the provenance and attribution metadata of substances, and the concise bioactivity data view of substances._


To use the Pubchem vocabulary include the following lines your namespace properties file:

```
com.bigdata.rdf.store.AbstractTripleStore.inlineURIFactory=com.blazegraph.vocab.pubchem.PubChemInlineURIFactory
com.bigdata.rdf.store.AbstractTripleStore.vocabularyClass=com.blazegraph.vocab.pubchem.PubChemVocabulary
```

###An example with Pubchem Core

Download the core Pubchem subset based on the [examples](https://pubchem.ncbi.nlm.nih.gov/rdf/#_Toc421254667).  For this example, we'll use `/path/to/pubchem-core/`.

```
#!/bin/sh
    wget -r -A ttl.gz -nH --cut-dirs=3 -P compound ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/compound/general
    wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/substance
    wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/descriptor
    wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/synonym
    wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/inchikey
    wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/bioassay
    wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/measuregroup
    wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/endpoint
    wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/protein
    wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/biosystem
    wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/conserveddomain
    wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/gene
    wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/source
    wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/concept
    wget -r -A ttl.gz -nH --cut-dirs=2 ftp://ftp.ncbi.nlm.nih.gov/pubchem/RDF/reference

```

Install and start your local Blazegraph instance.  See [Deploying](https://wiki.blazegraph.com/wiki/index.php/NanoSparqlServer#Deploying_NanoSparqlServer).

Use the BulkLoader with the example [pubchem properties](src/main/resources/namespace/pubchem/pubchem.properties), customized for your local environment.   Here is the example for using the [BulkLoader](https://wiki.blazegraph.com/wiki/index.php/REST_API#Bulk_Load_Configuration).

Update the file below `dataloader.xml` with your local configuration for `pubchem.properties` and the `/path/to/pubchem/core` as well as any other configuration for your local machine.

```
<?xml version="1.0" encoding="UTF-8" standalone="no"?> 
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd"> 
	  <properties>
	      <!-- RDF Format (Default is rdf/xml) --> 
	      <entry key="format">rdf/xml</entry> 
	      <!-- Base URI (Optional) --> 
	      <entry key="baseURI"></entry> 
	      <!-- Default Graph URI (Optional - Required for quads mode namespace) --> 
	      <entry key="defaultGraph"></entry> 
	      <!-- Suppress all stdout messages (Optional) --> 
	      <entry key="quiet">false</entry> 
	      <!-- Show additional messages detailing the load performance. (Optional) --> 
	      <entry key="verbose">0</entry> 
	     <!-- Compute the RDF(S)+ closure. (Optional) --> 
             <entry key="closure">false</entry> 
	     <!-- Files will be renamed to either .good or .fail as they are processed. 
                   The files will remain in the same directory. -->
	     <entry key="durableQueues">true</entry> 
	     <!-- The namespace of the KB instance. Defaults to kb. --> 
	     <entry key="namespace">pubchem</entry> 
	     <!-- The configuration file for the database instance. It must be readable by the web application. --> 
             <entry key="propertyFile">/path/to/pubchem.properties</entry> 
	     <!-- Zero or more files or directories containing the data to be loaded. 
                   This should be a comma delimited list. The files must be readable by the web application. -->
           <entry key="fileOrDirs">/path/to/pubchem-core/</entry>
      </properties>
```

Then post it into your running instance.

```
curl -X POST --data-binary @dataloader.xml --header 'Content-Type:application/xml' http://localhost:9999/blazegraph/dataloader
```

This will bulk load the namespace `pubchem` with the files from `/path/to/pubchem-core`.
