package com.bigdata.rdf.magic;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;

public class MagicTuple implements IMagicTuple {
    static long NULL = IRawTripleStore.NULL;
    
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
    
    public String toString() {

        StringBuilder sb = new StringBuilder();
        
        sb.append("< ");
        
        for (long l : terms) {
            sb.append(toString(l)).append(", ");
        }
        
        if (sb.length() > 2) {
            sb.setLength(sb.length()-2);
        }

        sb.append(" >");
        
        return sb.toString();

    }

    /**
     * Represents the term identifier together with its type (literal, bnode, uri,
     * or statement identifier).
     * 
     * @param id
     *            The term identifier.
     * @return
     */
    public static String toString(long id) {

        if (id == NULL)
            return "NULL";

        if (AbstractTripleStore.isLiteral(id))
            return id + "L";

        if (AbstractTripleStore.isURI(id))
            return id + "U";

        if (AbstractTripleStore.isBNode(id))
            return id + "B";

        if (AbstractTripleStore.isStatement(id))
            return id + "S";

        throw new AssertionError("id="+id);
        
    }

}
