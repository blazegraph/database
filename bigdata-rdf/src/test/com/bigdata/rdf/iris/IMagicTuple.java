package com.bigdata.rdf.iris;

public interface IMagicTuple {
    
    long getTerm(int index);
    
    int getTermCount();
    
    long[] getTerms();
    
    boolean isFullyBound();
    
}
