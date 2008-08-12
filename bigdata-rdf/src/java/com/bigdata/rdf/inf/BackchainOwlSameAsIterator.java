package com.bigdata.rdf.inf;

import java.util.Set;
import java.util.TreeSet;

import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.striterator.IChunkedOrderedIterator;

public abstract class BackchainOwlSameAsIterator implements IChunkedOrderedIterator<SPO> {
    
    protected final static transient long NULL = IRawTripleStore.NULL;
    
    /**
     * The database.
     */
    protected AbstractTripleStore db;

    /**
     * This flag is <code>true</code> since we do NOT want statement
     * identifiers to be generated for inferences produced by the backchainer.
     */
    protected final boolean copyOnly = true;
    
    protected long sameAs;

    protected IChunkedOrderedIterator<SPO> src;

    public BackchainOwlSameAsIterator(IChunkedOrderedIterator<SPO> src, AbstractTripleStore db,
            long sameAs) {
        if (src == null)
            throw new IllegalArgumentException();
        if (db == null)
            throw new IllegalArgumentException();
        if (sameAs == NULL)
            throw new IllegalArgumentException();
        this.src = src;
        this.db = db;
        this.sameAs = sameAs;
    }

    public Set<Long> getSelfAndSames(long id) {
        Set<Long> selfAndSames = new TreeSet<Long>();
        selfAndSames.add(id);
        getSames(id, selfAndSames);
        return selfAndSames;
    }

    public Set<Long> getSames(long id) {
        Set<Long> sames = new TreeSet<Long>();
        sames.add(id);
        getSames(id, sames);
        sames.remove(id);
        return sames;
    }

    public void getSames(long id, Set<Long> sames) {
        IChunkedOrderedIterator<SPO> it = db.getAccessPath(id, sameAs, NULL).iterator();
        try {
            while (it.hasNext()) {
                long same = it.next().o;
                if (!sames.contains(same)) {
                    sames.add(same);
                    getSames(same, sames);
                }
            }
        } finally {
            it.close();
        }
        it = db.getAccessPath(NULL, sameAs, id).iterator();
        try {
            while (it.hasNext()) {
                long same = it.next().s;
                if (!sames.contains(same)) {
                    sames.add(same);
                    getSames(same, sames);
                }
            }
        } finally {
            it.close();
        }
    }
}
