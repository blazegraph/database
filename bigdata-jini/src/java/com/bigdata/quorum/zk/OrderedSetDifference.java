package com.bigdata.quorum.zk;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Compute the change in an ordered set.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 */
class OrderedSetDifference<T> {

    private final List<T> added = new LinkedList<T>();

    private final List<T> removed = new LinkedList<T>();

    /**
     * The things which were added to the set (immutable).
     */
    public List<T> added() {
        return Collections.unmodifiableList(added); 
    }

    /**
     * The things which were removed form the set (immutable).
     */
    public List<T> removed() {
        return Collections.unmodifiableList(removed); 
    }

    /**
     * For ordered set difference we take a co-routine approach
     * either adding members from the old to the remove set or
     * adding members from the new to the added.
     * <p>
     * Traverse the new list, removing members from old or adding members
     * from the new if a match cannot be found.
     * 
     * @param aold
     *            The old set.
     * @param anew
     *            The new set.
     */
    public OrderedSetDifference(final T[] aold, final T[] anew) {

    	// boundary condition if new list is empty
    	if (anew.length == 0) {
    		for (int i = 0; i < aold.length; i++) {
				removed.add(aold[i]);
    		}   		
    	} else {
	        int oi = 0;
	        
	        for (int i = 0; i < anew.length; i++) {
	        	boolean found = false;
	    		while (!found && oi < aold.length) {
	    			if (aold[oi].equals(anew[i])) {
	    				found = true;
	    			} else {
	    				removed.add(aold[oi]);
	    			}
	    			
	    			oi++;
	    		}
	    		
	        	if (!found) {
	        		added.add(anew[i]); // none left in old, so must be added
	        	}
	        }
    	}
    }

    public String toString() {

        return getClass().getName() + //
                "{removed=" + removed + //
                ",added=" + added + //
                "}";
        
    }
    
}