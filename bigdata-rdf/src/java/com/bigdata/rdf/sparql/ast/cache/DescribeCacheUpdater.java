package com.bigdata.rdf.sparql.ast.cache;

import info.aduna.iteration.CloseableIteration;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.rio.RDFWriter;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.eval.ASTConstructIterator;
import com.bigdata.rdf.store.TempTripleStore;

/**
 * Collects statements written onto the {@link RDFWriter} interface and
 * adds/replaces the DESCRIBE of the {@link Resource} specified to the
 * constructor.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
public class DescribeCacheUpdater implements
        CloseableIteration<BigdataStatement, QueryEvaluationException> {

    static private transient final Logger log = Logger
            .getLogger(DescribeCacheUpdater.class);

    /**
     * The cache to be updated.
     */
    private final IDescribeCache cache;

    /**
     * The {@link BigdataValue}s that become bound for the projection of the
     * original DESCRIBE query. We will collect all statements having a
     * described resource as either a subject or an object.
     * <p>
     * Note: This set is populated as the solutions are observed before they are
     * fed into the {@link ASTConstructIterator}. It MUST be a thread-safe
     * {@link Set} in order to ensure the visibility of the updates to this
     * class. It should also support high concurrency.
     */
    private final Set<BigdataValue> describedResources;
    
    /**
     * The source iterator visiting the statements that are the description
     * of the projected resources.
     */
    private final CloseableIteration<BigdataStatement, QueryEvaluationException> src;
    
    /**
     * The statements to be inserted into the cache as the description of
     * that {@link IV}.
     * 
     * TODO This is not scalable to very large numbers of described
     * resources nor to resources with very large numbers of statements in
     * their descriptions. Try {@link TempTripleStore} with ONE (1) access
     * path on SPO. However, we want to have the {@link BigdataStatement}
     * with its {@link IV}s and its {@link Value}s, so the
     * {@link TempTripleStore} will not work. Something more custom?
     */
    final private HashMap<BigdataValue, Graph> graphs = new HashMap<BigdataValue, Graph>();

    private boolean open = true;
    
    /**
     * 
     * @param cache
     *            The cache to be updated.
     * @param describedResources
     *            The {@link BigdataValue}s that become bound for the projection
     *            of the original DESCRIBE query. We will collect all statements
     *            having a described resource as either a subject or an object.
     *            This MUST be a thread-safe (and concurrency favorable) set in
     *            order to ensure the visibility of the updates.
     * @param src
     *            The source iterator, visiting the statements that are the
     *            description of the resource(s) identified in the
     *            {@link ProjectionNode}.
     */
    public DescribeCacheUpdater(
            final IDescribeCache cache,
            final Set<BigdataValue> describedResources,
            final CloseableIteration<BigdataStatement, QueryEvaluationException> src) {

        if (cache == null)
            throw new IllegalArgumentException();

        if (describedResources == null)
            throw new IllegalArgumentException();

        if (src == null)
            throw new IllegalArgumentException();

        this.cache = cache;

        this.describedResources = describedResources;

        this.src = src;

    }

    @Override
    public boolean hasNext() throws QueryEvaluationException {

        if (src.hasNext())
            return true;

        if (open) {

            try {

                /*
                 * Update the DESCRIBE cache IFF the iterator is exhausted
                 * by normal means (versus a thrown exception from the
                 * source iterator).
                 */

                updateCache();

            } finally {

                // Close the iterator regardless.
                close();

            }

        }

        return false;

    }

    /**
     * TODO In order to support CBD, we will also have to recognize
     * statements that describe blank nodes that are part of the description
     * of a described resource as belonging to that described resource. This
     * is necessary in order to capture the transitive closure of the
     * resource description specified by CBD. The code in this method only
     * recognizes statements that directly have a described resource as a
     * subject or object. We probably need a reverse map that will allow us
     * to navigate from a BigdataValue (or perhaps just a BigdataBNode) to
     * all described resources for which that value was observed. That map
     * might only need to contain the blank nodes since the description can
     * never expand beyond a statement having a blank node in the subject
     * (or object) position and a non-blank node in the object (or subject)
     * position.
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/578">
     *      Concise Bounded Description </a>
     */
    @Override
    public BigdataStatement next() throws QueryEvaluationException {

        // A statement produced by the CONSTRUCT iterator.
        final BigdataStatement stmt = src.next();

        // Check the Subject.
        {
            
            final BigdataValue s = stmt.getSubject();

            // Is the subject one of the described resources?
            if (describedResources.contains(s)) {

                record(s, stmt);

            }
            
        }

        // Check the Object.
        {
            final BigdataValue o = stmt.getObject();

            // Is the object one of the described resources?
            if (describedResources.contains(o)) {

                record(o, stmt);

            }
            
        }
        
        return stmt;

    }

    /**
     * Associate the statement with the resource. It is part of the
     * description of that resource.
     * 
     * @param describedResource
     *            A resource that is being described.
     * @param stmt
     *            A statement having that resource as either the subject or
     *            object.
     */
    private void record(final BigdataValue describedResource,
            final BigdataStatement stmt) {

        Graph g = graphs.get(describedResource);
        
        if(g == null) {
            
            graphs.put(describedResource, g = new GraphImpl());

        }

        g.add(stmt);

        if (log.isDebugEnabled())
            log.debug("DESCRIBE: describedResource=" + describedResource
                    + ", statement=" + stmt);

    }

    private void updateCache() {

        for (Map.Entry<BigdataValue, Graph> e : graphs.entrySet()) {

            final BigdataValue describedResource = e.getKey();

            final IV<?, ?> iv = describedResource.getIV();

            if (iv == null)
                throw new AssertionError("IV not set: " + describedResource);
            
            final Graph graph = e.getValue();
            
            cache.insert(iv, graph);

            if (log.isInfoEnabled())
                log.info("DESCRIBE UPDARTE: describedResource="
                        + describedResource + ", graph=" + graph);

        }
        
    }
    
    @Override
    public void close() throws QueryEvaluationException {

        if(open) {
        
            src.close();
            
            open = false;
            
        }

    }
    
    @Override
    public void remove() throws QueryEvaluationException {

        throw new UnsupportedOperationException();
        
    }

}