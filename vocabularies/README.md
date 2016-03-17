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
