package com.bigdata.rdf.magic;

import com.bigdata.rdf.internal.IV;

public interface IMagicTuple {
    
    IV getTerm(int index);
    
    int getTermCount();
    
    IV[] getTerms();
    
    boolean isFullyBound();
    
}
