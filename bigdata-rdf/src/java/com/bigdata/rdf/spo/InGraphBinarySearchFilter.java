package com.bigdata.rdf.spo;

import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

import org.openrdf.model.URI;

import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.store.IRawTripleStore;

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
 */
public final class InGraphBinarySearchFilter extends SPOFilter
        implements Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = -3566012247356882422L;

    /**
     * Note: Not final since the class implements {@link Externalizable}.
     */
    private long[] a;

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

        final LongLinkedOpenHashSet contextSet = new LongLinkedOpenHashSet();
        
        for (URI uri : graphs) {
        
            final long termId = ((BigdataURI) uri).getTermId();
            
            if (termId != IRawTripleStore.NULL) {

                contextSet.add(termId);
                
            }
            
        }
        
        a = contextSet.toArray(new long[0]);
        
        Arrays.sort(a);
        
    }

    public boolean accept(final ISPO spo) {
        
        return Arrays.binarySearch(a, spo.c()) >= 0;
        
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        
        final int size = in.readInt();
        
        a = new long[size];
        
        for(int i=0; i<size; i++) {
            
            a[i] = in.readLong();
            
        }
        
    }

    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeInt(a.length);
        
        for(long id : a) {
            
            out.writeLong(id);
            
        }
        
    }

}