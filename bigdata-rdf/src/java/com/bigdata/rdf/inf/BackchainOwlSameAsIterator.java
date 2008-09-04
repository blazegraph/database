package com.bigdata.rdf.inf;

import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.striterator.IChunkedOrderedIterator;

public abstract class BackchainOwlSameAsIterator implements IChunkedOrderedIterator<ISPO> {
    
    protected final static Logger log = Logger.getLogger(BackchainOwlSameAsIterator.class);
    
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

    protected IChunkedOrderedIterator<ISPO> src;
    
    public BackchainOwlSameAsIterator(IChunkedOrderedIterator<ISPO> src,
            AbstractTripleStore db, long sameAs) {

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
    
    protected Set<Long> getSelfAndSames(long id) {
        Set<Long> selfAndSames = new TreeSet<Long>();
        selfAndSames.add(id);
        getSames(id, selfAndSames);
        return selfAndSames;
    }

    protected Set<Long> getSames(long id) {
        Set<Long> sames = new TreeSet<Long>();
        sames.add(id);
        getSames(id, sames);
        sames.remove(id);
        return sames;
    }

    protected void getSames(long id, Set<Long> sames) {
        IChunkedOrderedIterator<ISPO> it = db.getAccessPath(id, sameAs, NULL).iterator();
        try {
            while (it.hasNext()) {
                long same = it.next().o();
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
                long same = it.next().s();
                if (!sames.contains(same)) {
                    sames.add(same);
                    getSames(same, sames);
                }
            }
        } finally {
            it.close();
        }
    }
    
    protected TempTripleStore createTempTripleStore() {
        // log.info("creating temp triple store for owl:sameAs backchainer");
        // System.err.println("creating temp triple store for owl:sameAs backchainer");
        Properties props = db.getProperties();
        // do not store terms
        props.setProperty(AbstractTripleStore.Options.LEXICON, "false");
        // only store the SPO index
        props.setProperty(AbstractTripleStore.Options.ONE_ACCESS_PATH, "true");
        return new TempTripleStore(db.getIndexManager().getTempStore(), props, db);
    }

    protected void dumpSPO(ISPO spo) {
//        System.err.println(spo.toString(db));
    }
    
}
