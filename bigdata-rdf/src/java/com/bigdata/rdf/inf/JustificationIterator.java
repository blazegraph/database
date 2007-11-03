package com.bigdata.rdf.inf;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;

/**
 * Fully buffers and then visits all {@link Justification}s for a given
 * statement.
 * 
 * @todo add the chunked api methods to this class (abstract the chunked
 *       iterator into a generic interface). we will not need to fully buffer
 *       once we convert to use the concurrent journal.
 *       <p>
 *       Note: efficient remove will have to buffer the remove requests and then
 *       send them off in a batch to a data service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JustificationIterator implements Iterator<Justification> {

    /** the database. */
    private final AbstractTripleStore db;
    
    /** the statement whose justifications are being visited. */
    private final SPO head;

    /**
     * Private key builder.
     * <p>
     * Note: This capacity estimate is based on N longs per SPO, one head,
     * and 2-3 SPOs in the tail. The capacity will be extended automatically
     * if necessary.
     */
    private KeyBuilder keyBuilder = new KeyBuilder(IRawTripleStore.N * (1 + 3) * Bytes.SIZEOF_LONG);

    /** the index in which the justifications are stored. */
    private final IIndex ndx;
    private final Justification[] justifications;
    private final int numJustifications;
    
    private boolean open = true;
    private int i = 0;
    private Justification current = null;
    
    /**
     * 
     * @param db
     * @param head The statement whose justifications will be materialized.
     */
    public JustificationIterator(AbstractTripleStore db, SPO head) {
        
        assert db != null;
        
        assert head != null;
        
        this.db = db;
        
        this.head = head;
        
        this.ndx = db.getJustificationIndex();
        
        byte[] fromKey = keyBuilder.reset().append(head.s).append(head.p)
                .append(head.o).getKey();

        byte[] toKey = keyBuilder.reset().append(head.s).append(head.p)
                .append(head.o + 1).getKey();
        
        final int rangeCount = ndx.rangeCount(fromKey,toKey);

        this.justifications = new Justification[ rangeCount ];

        /*
         * Materialize the matching justifications.
         */
        
        IEntryIterator itr = ndx.rangeIterator(fromKey,toKey);

        int i = 0;

        while (itr.hasNext()) {

            itr.next();

            Justification jst = new Justification(itr.getKey());;

            // @todo comment out and make ids private.
            assert jst.ids[0] == head.s;
            assert jst.ids[1] == head.p;
            assert jst.ids[2] == head.o;
            
            justifications[i++] = jst;
            
        }

        this.numJustifications = i;
        
    }
    
    public boolean hasNext() {

        if(!open) return false;
        
        assert i <= numJustifications;
        
        if (i == numJustifications) {

            return false;
            
        }

        return true;
        
    }

    public Justification next() {
        
        if (!hasNext()) {

            throw new NoSuchElementException();
        
        }
        
        current = justifications[i++];
        
        return current;
        
    }

    /**
     * Removes the last {@link Justification} visited from the database
     * (non-batch API).
     */
    public void remove() {

        if (!open)
            throw new IllegalStateException();

        if(current==null) {
            
            throw new IllegalStateException();
            
        }
        
        /*
         * Remove the justifications from the store (note that there is no value
         * stored under the key).
         */

        ndx.remove(current.getKey(keyBuilder));
        
    }

}
