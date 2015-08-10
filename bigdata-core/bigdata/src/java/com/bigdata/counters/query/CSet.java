package com.bigdata.counters.query;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterNode;

/**
 * Pairs together an ordered set of category values for a pivot table with the
 * set of counters which share that set of category values.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class CSet {

    /**
     * The set of ordered category values.
     */
    public final String[] cats;

    /**
     * The set of counters sharing the same set of ordered category values.
     */
    public final List<ICounter> counters;

    public String toString() {

// final StringBuilder sb = new StringBuilder();
//
//            int i = 0;
//
//            for (ICounter c : counters) {
//
//                if (i > 0)
//                    sb.append(",");
//
//                // @todo use c.getPath() if you want to see WHICH counter was included.
//                sb.append(c.getName());
//
//                i++;
//                
//            }
            
        return "CSet{cats=" + Arrays.toString(cats) + //
                ", #counters="+counters.size()+//
                // Note: This is just too much detail.
                //",counters=[" + sb + "]" + //
                "}";
        
    }

    /**
     * Create a set based on the specified category values and initially
     * containing the specified {@link ICounter}.
     * 
     * @param cats
     *            An ordered set of category values.
     * @param counter
     *            A counter from whose {@link ICounterNode#getPath()} the
     *            category values were extracted as capturing groups.
     */
    public CSet(final String[] cats, final ICounter counter) {

        if (cats == null)
            throw new IllegalArgumentException();

        if (counter == null)
            throw new IllegalArgumentException();

        this.cats = cats;

        this.counters = new LinkedList<ICounter>();

        add(counter);

    }

    /**
     * Add another counter to that set.
     * 
     * @param counter
     *            The counter.
     */
    public void add(final ICounter counter) {

        if (counter == null)
            throw new IllegalArgumentException();

        counters.add(counter);

    }

}
