package com.bigdata.objectIndex;

import java.io.Serializable;

import com.bigdata.journal.SimpleObjectIndex.IObjectIndexEntry;

/**
 * An entry in a {@link Leaf}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Reconcile with {@link IObjectIndexEntry} and {@link NodeSerializer}.
 */
public class Entry implements Serializable {

    private static final long serialVersionUID = 1L;

    private static int nextId = 1;
    private int id;
    
    /**
     * Create a new entry.
     */
    public Entry() {
        id = nextId++;
    }

    /**
     * Copy constructor.
     * 
     * @param src
     *            The source to be copied.
     */
    public Entry(Entry src) {
        id = src.id;
    }

    public String toString() {
        return ""+id;
    }
    
}