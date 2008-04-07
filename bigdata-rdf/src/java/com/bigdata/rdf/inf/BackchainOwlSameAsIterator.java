package com.bigdata.rdf.inf;

import java.util.Set;
import java.util.TreeSet;
import com.bigdata.rdf.spo.ISPOIterator;
import com.bigdata.rdf.store.AbstractTripleStore;

public abstract class BackchainOwlSameAsIterator implements ISPOIterator {
    protected AbstractTripleStore db;

    protected long sameAs;

    protected ISPOIterator src;

    public BackchainOwlSameAsIterator(ISPOIterator src, AbstractTripleStore db,
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
        ISPOIterator it = db.getAccessPath(id, sameAs, NULL).iterator();
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
