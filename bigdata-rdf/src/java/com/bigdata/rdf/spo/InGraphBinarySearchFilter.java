package com.bigdata.rdf.spo;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.HashSet;
import org.openrdf.model.URI;

import com.bigdata.bop.constraint.INBinarySearch;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.relation.rule.eval.ISolution;

/**
 * "IN" filter for the context position based on a sorted long[] of the
 * acceptable graph identifiers. While evaluation of the access path will be
 * ordered, the filter does not maintain evolving state so a hash set will
 * likely beat a binary search.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 * 
 * @see InGraphHashSetFilter
 * 
 * @todo reconcile with {@link INBinarySearch}
 */
public final class InGraphBinarySearchFilter<E extends ISPO> extends SPOFilter<E>
        implements Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = -3566012247356882422L;

    /**
     * Note: Not final since the class implements {@link Externalizable}.
     */
    private IV[] a;

    /**
     * Deserialization constructor.
     */
    public InGraphBinarySearchFilter() {
        
    }
    
    /**
     * 
     * @param graphs
     *            The set of acceptable graph identifiers.
     */
    public InGraphBinarySearchFilter(final Iterable<? extends URI> graphs) {

        /*
         * Create a sorted array of term identifiers for the set of contexts
         * we will accept.
         */

        final HashSet<IV> contextSet = new HashSet<IV>();
        
        for (URI uri : graphs) {
        
            final IV termId = ((BigdataURI) uri).getIV();
            
            if (termId != null) {

                contextSet.add(termId);
                
            }
            
        }
        
        a = contextSet.toArray(new IV[0]);
        
        Arrays.sort(a);
        
    }

    public boolean isValid(Object o) {
        
        if (!canAccept(o)) {
            
            return true;
            
        }
        
        final ISolution solution = (ISolution) o;
        
        return accept((ISPO) solution.get());
        
    }

    private boolean accept(final ISPO o) {
        
        final ISPO spo = (ISPO) o;
        
        return Arrays.binarySearch(a, spo.c()) >= 0;
        
    }

    /**
     * The initial version.
     */
    private static final transient short VERSION0 = 0;

    /**
     * The current version.
     */
    private static final transient short VERSION = VERSION0;

    public void readExternal(final ObjectInput in) throws IOException,
            ClassNotFoundException {

        final short version = in.readShort();

        switch (version) {
        case VERSION0:
            break;
        default:
            throw new UnsupportedOperationException("Unknown version: "
                    + version);
        }
        
        final int size = in.readInt();
        
        a = new IV[size];
        
        for(int i=0; i<size; i++) {
            
            a[i] = (IV) in.readObject();
            
        }
        
    }

    public void writeExternal(final ObjectOutput out) throws IOException {

        out.writeShort(VERSION);
        
        out.writeInt(a.length);
        
        for(IV iv : a) {
            
            out.writeObject(iv);
            
        }
        
    }

}