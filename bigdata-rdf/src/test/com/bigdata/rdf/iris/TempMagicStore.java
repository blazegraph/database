package com.bigdata.rdf.iris;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLock;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.RelationSchema;

public class TempMagicStore extends TempTripleStore {

    protected static final Logger log = Logger.getLogger(TempMagicStore.class);
    
    protected static final boolean INFO = log.isInfoEnabled();
    
    public TempMagicStore(IIndexManager indexManager, String namespace,
            Long timestamp, Properties properties) {
        super(indexManager, namespace, timestamp, properties);
    }

    public TempMagicStore(Properties properties) {
        super(properties);
    }

    public TempMagicStore(TemporaryStore store, Properties properties,
            AbstractTripleStore db) {
        super(store, properties, db);
    }

    @Override
    public Iterator<IRelation> relations() {

        Collection<String> symbols = getMagicSymbols();
        IRelation[] relations = 
            new IRelation[symbols.size()+ (lexicon ? 2 : 1)];
        int i = 0; 
        relations[i++] = getSPORelation();
        if (lexicon) {
            relations[i++] = getLexiconRelation();
        }
        for (String symbol : symbols) {
            relations[i++] = getMagicRelation(symbol);
        }
        return Collections.unmodifiableList(
            Arrays.asList(relations)).iterator();
        
    }
    
    public MagicRelation createRelation(String symbol, int arity) {
        
        MagicRelation relation = getMagicRelation(symbol);
        if (relation != null) {
            return relation;
        }
        
        assertWritable();
        
        final Properties tmp = getProperties();
        
        // set property that will let the contained relations locate their container.
        tmp.setProperty(RelationSchema.CONTAINER, getNamespace());
        // set the arity (REQUIRED)
        tmp.setProperty(MagicSchema.ARITY, String.valueOf(arity));
        
        final IResourceLock resourceLock = acquireExclusiveLock();

        try {

            relation = new MagicRelation(getIndexManager(), 
                getNamespace() + "." + symbol, getTimestamp(), tmp);

            relation.create();

            /*
             * Update the global row store to set the axioms and the
             * vocabulary objects.
             */
            {

                Collection<String> symbols = getMagicSymbols();
                
                symbols.add(symbol);
                
                final Map<String, Object> map = new HashMap<String, Object>();

                // primary key.
                map.put(RelationSchema.NAMESPACE, getNamespace());

                // symbols.
                map.put(MagicSchema.SYMBOLS, symbols);

                // Write the map on the row store.
                getIndexManager().getGlobalRowStore().write(
                        RelationSchema.INSTANCE, map);

            }
        
            /*
             * Note: A commit is required in order for a read-committed view to
             * have access to the registered indices.
             * 
             * @todo have the caller do this? It does not really belong here
             * since you can not make a large operation atomic if you do a
             * commit here.
             */

            commit();

        } finally {

            unlock(resourceLock);

        }
        
        return relation;

    }
    
    final public synchronized Collection<String> getMagicSymbols() {
        
        /*
         * Extract the de-serialized symbols from the global row
         * store.
         */
        
        Collection<String> symbols = (Collection<String>) 
            getIndexManager().getGlobalRowStore().get(
                RelationSchema.INSTANCE, getNamespace(),
                MagicSchema.SYMBOLS);
        
        if (symbols == null) {
            symbols = new LinkedList<String>();
        }

        return symbols;
        
    }
    
    final public synchronized MagicRelation getMagicRelation(String symbol) {
        
        return (MagicRelation) getIndexManager().getResourceLocator()
                .locate(getNamespace() + "." + symbol,
                        getTimestamp());

    }
    
    @Override
    public void destroy() {
    
        assertWritable();

        final IResourceLock resourceLock = acquireExclusiveLock();

        try {

            Collection<String> symbols = getMagicSymbols();
            
            for (String symbol : symbols) {
                
                MagicRelation relation = getMagicRelation(symbol);
                
                if (INFO) log.info("destroying relation: " + relation.getNamespace());
                
                relation.destroy();
                
            }
            
            super.destroy();
            
        } finally {

            unlock(resourceLock);
            
        }
        
    }
    
}
