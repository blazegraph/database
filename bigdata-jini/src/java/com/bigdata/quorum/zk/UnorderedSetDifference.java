package com.bigdata.quorum.zk;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Compute the change in an unordered set.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 */
class UnorderedSetDifference<T extends Comparable<T>> {

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
     * 
     * @param aold
     *            The old set.
     * @param anew
     *            The new set.
     */
    public UnorderedSetDifference(final T[] aold, final T[] anew) {

        // sort both arrays.
        Arrays.sort(aold);
        Arrays.sort(anew);
        {
            // look for members that left.
            for (int i = 0; i < aold.length; i++) {
                boolean found = false;
                // consider an old member.
                final T t = aold[i];
                // look at the new members.
                for (T u : anew) {
                    if (t.equals(u)) {
                        // old member is still present.
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    // in old, but not in new.
                    removed.add(t);
                }
            }
        }
        {
            // look for new members.
            for (int i = 0; i < anew.length; i++) {
                boolean found = false;
                // consider a new member.
                final T t = anew[i];
                // look at the old members.
                for (T u : aold) {
                    if (t.equals(u)) {
                        // new member was already known.
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    // in new, but not in old.
                    added.add(t);
                }
            }
        }
    }

}