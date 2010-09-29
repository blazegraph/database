package com.bigdata.rdf.spo;

import java.util.HashSet;
import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet;

import org.openrdf.model.URI;

import com.bigdata.bop.constraint.INHashMap;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.rule.eval.ISolution;

/**
 * "IN" filter for the context position based on a native long hash set
 * containing the acceptable graph identifiers. While evaluation of the
 * access path will be ordered, the filter does not maintain evolving state
 * so a hash set will likely beat a binary search.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 * 
 * @see InGraphBinarySearchFilter
 * 
 * @todo reconcile with {@link INHashMap}.
 * 
 * @todo tighten serialization?
 */
public final class InGraphHashSetFilter<E extends ISPO> extends SPOFilter<E> {

    /**
     * 
     */
    private static final long serialVersionUID = -6059009162692785772L;

    final HashSet<IV> contextSet;
    
    /**
     * 
     * @param graphs
     *            The set of acceptable graph identifiers.
     */
    public InGraphHashSetFilter(final int initialCapacity,
            final Iterable<? extends URI> graphs) {

        /*
         * Create a sorted array of term identifiers for the set of contexts
         * we will accept.
         */

        contextSet = new HashSet<IV>(initialCapacity);
        
        for (URI uri : graphs) {
        
            final IV termId = ((BigdataURI) uri).getIV();
            
            if (termId != null) {

                contextSet.add(termId);
                
            }
            
        }
        
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
        
        return contextSet.contains(spo.c());
        
    }

}