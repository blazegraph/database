package com.bigdata.rdf.magic;

public interface IMagicTuple {
    
    long getTerm(int index);
    
    int getTermCount();
    
    long[] getTerms();
    
    boolean isFullyBound();
    
}
