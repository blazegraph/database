package com.bigdata.relation.rdf;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.service.IBigdataFederation;

/** FIXME integrate with RDF module. */
public class TestTripleStore {
    
    protected final static Logger log = Logger.getLogger(TestTripleStore.class);

    final private IBigdataFederation fed;
    final private String namespace; 
    final private long timestamp;
    private final SPORelation spoRelation;
    
    public TestTripleStore(IBigdataFederation fed, String namespace,
            long timestamp, Properties properties) {

        if (fed == null)
            throw new IllegalArgumentException();

        if (namespace == null)
            throw new IllegalArgumentException();

        this.fed = fed;

        this.namespace = namespace;

        this.timestamp = timestamp;

        this.spoRelation = new SPORelation(fed.getThreadPool(), fed, namespace,
                timestamp, properties);

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
     * The service used for running iterators, index writes, and rules in
     * parallel.
     */
    public ExecutorService getThreadPool() {
        
        return fed.getThreadPool();
        
    }
    
    synchronized public SPORelation getSPORelation() {
    
        return spoRelation;

    }
    
    public void create() {
        
        getSPORelation().create();
        
    }

    public void destroy() {

        getSPORelation().destroy();

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
