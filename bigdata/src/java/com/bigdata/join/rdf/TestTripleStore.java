package com.bigdata.join.rdf;

import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.join.IChunkedOrderedIterator;
import com.bigdata.service.IBigdataClient;

/** FIXME integrate with RDF module. */
public class TestTripleStore {
    
    protected final static Logger log = Logger.getLogger(TestTripleStore.class);
    
//    public interface Options {
//        
//        /**
//         * The namespace for the indices used by the {@link TestTripleStore}.
//         */
//        String NAMESPACE = Options.class.getPackage().toString() + ".namespace";
//        
//    };

    final private IBigdataClient client;
    final private String namespace; 
    final private long timestamp;
    
    public TestTripleStore(IBigdataClient client, String namespace, long timestamp) {

        if (client == null)
            throw new IllegalArgumentException();
        
        this.client = client;
        
//        final Properties properties = client.getProperties();
        
//        namespace = properties.getProperty(Options.NAMESPACE);

        if (namespace == null)
            throw new IllegalArgumentException();
        
        this.namespace = namespace;
        
        this.timestamp = timestamp;
        
    }

    /**
     * Return the fully qualified index name.
     * 
     * @param name
     *            The basename of the index.
     *            
     * @return The actual index name.
     */
    protected String getFQN(String name) {

        return namespace + name;
        
    }

    /**
     * The namespace for the indices used by this instance.
     */
    public String getNamespace() {
        
        return namespace;
        
    }
    
    /**
     * Return the client's thread pool. This is used for running iterators,
     * index writes, and rules in parallel.
     */
    public ExecutorService getThreadPool() {
        
        return client.getFederation().getThreadPool();
        
    }
    
    /**
     * Return the statement index for the given {@link SPOKeyOrder}.
     * 
     * @todo move to {@link SPORelation}?
     */
    public IIndex getStatementIndex(SPOKeyOrder keyOrder) {
        
        return client.getFederation().getIndex(getFQN(keyOrder.getIndexName()), timestamp);

    }

    synchronized public SPORelation getSPORelation() {
    
        if (spoRelation == null) {

            spoRelation = new SPORelation(this);

        }

        return spoRelation;

    }
    private SPORelation spoRelation;
    
    protected IndexMetadata newStatementIndexMetadata(SPOKeyOrder keyOrder) {
        
        IndexMetadata md = new IndexMetadata(getFQN(keyOrder.getIndexName()),
                UUID.randomUUID());
        
        md.setTupleSerializer(new SPOTupleSerializer(keyOrder));
        
        return md;
        
    }
    
    public void create() {
        
        client.getFederation().registerIndex(
                newStatementIndexMetadata(SPOKeyOrder.SPO));

        client.getFederation().registerIndex(
                newStatementIndexMetadata(SPOKeyOrder.POS));

        client.getFederation().registerIndex(
                newStatementIndexMetadata(SPOKeyOrder.OSP));

    }

    public void destroy() {

        client.getFederation()
                .dropIndex(getFQN(SPOKeyOrder.SPO.getIndexName()));

        client.getFederation()
                .dropIndex(getFQN(SPOKeyOrder.POS.getIndexName()));

        client.getFederation()
                .dropIndex(getFQN(SPOKeyOrder.OSP.getIndexName()));

    }

    public StringBuilder dump() {
        
        final StringBuilder sb = new StringBuilder();
        
        // dump the SPO relation.
        {
        
            final IChunkedOrderedIterator<SPO> itr = getSPORelation()
                    .getAccessPath(0L, 0L, 0L).iterator();

            try {

                while (itr.hasNext()) {

                    sb.append(itr.next());

                    sb.append("\n");

                }
                
            } finally {

                itr.close();

            }

        }

        return sb;

    }

}
