package com.bigdata.rdf.magic;

import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.internal.VTE;

public class MagicTuple implements IMagicTuple {
    private IV[] terms;

    public MagicTuple(int... terms) {
        IV[] ivs = new IV[terms.length];
        for (int i = 0; i < terms.length; i++) {
            ivs[i] = new TermId(VTE.URI, terms[i]);
        }
        this.terms = ivs;
    }
    
    public MagicTuple(IV... terms) {
        this.terms = terms;
    }
    
    public MagicTuple(IPredicate<IMagicTuple> pred) {
        terms = new IV[pred.arity()];
        for (int i = 0; i < pred.arity(); i++) {
            final IVariableOrConstant<IV> t = pred.get(i);
            terms[i] = t.isVar() ? null : t.get();
        }
    }

    public IV getTerm(int index) {
        if (index < 0 || index >= terms.length) {
            throw new IllegalArgumentException();
        }
        return terms[index];
    }

    public int getTermCount() {
        return terms.length;
    }

    public IV[] getTerms() {
        return terms;
    }
    
    public boolean isFullyBound() {
        for (IV term : terms) {
            if (term == null) {
                return false;
            }
        }
        return true;
    }
    
    public String toString() {

        StringBuilder sb = new StringBuilder();
        
        sb.append("< ");
        
        for (IV l : terms) {
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
    public static String toString(IV id) {

        if (id == null)
            return "NULL";

        return id.toString();
        
    }

}
