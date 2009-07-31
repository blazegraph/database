package com.bigdata.rdf.iris;

import com.bigdata.rdf.store.IRawTripleStore;

public class MagicTuple implements IMagicTuple {
    long NULL = IRawTripleStore.NULL;
    
    private long[] terms;

    public MagicTuple(long... terms) {
        this.terms = terms;
    }

    public long getTerm(int index) {
        if (index < 0 || index >= terms.length) {
            throw new IllegalArgumentException();
        }
        return terms[index];
    }

    public int getTermCount() {
        return terms.length;
    }

    public long[] getTerms() {
        return terms;
    }
    
    public boolean isFullyBound() {
        for (long term : terms) {
            if (term == NULL) {
                return false;
            }
        }
        return true;
    }
}
