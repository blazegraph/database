package com.bigdata.join.rdf;

import com.bigdata.btree.IIndex;

/** FIXME integrate with RDF module. */
public interface AbstractTripleStore {
    
    public IIndex getStatementIndex(SPOKeyOrder keyOrder);
    
}