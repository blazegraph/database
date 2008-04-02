package com.bigdata.text;

import java.util.Collection;
import java.util.Iterator;

/**
 * Visits search results in order of decreasing relevance.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Hiterator implements Iterator<Hit> {

    final private Collection<Hit> hits;
    final private Iterator<Hit> src;
    
    public Hiterator(Collection<Hit> hits) {
        
        if (hits == null)
            throw new IllegalArgumentException();

        this.hits = hits;
        
        // FIXME this is not ordered yet!
        this.src = hits.iterator();
        
    }

    /**
     * The #of hits (approximate).
     * 
     * @todo this and other search engine metadata (elapsed time) might go
     *       on a different object from which we can obtain the
     *       {@link Hiterator}.
     */
    public long size() {
        
        return hits.size();
        
    }
    
    public boolean hasNext() {

        return src.hasNext();
        
    }

    public Hit next() {
     
        return src.next();
        
    }

    // @todo should this even be supported?
    public void remove() {
        
        src.remove();
        
    }
    
}