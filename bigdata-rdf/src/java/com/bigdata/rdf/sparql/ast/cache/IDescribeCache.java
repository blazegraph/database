package com.bigdata.rdf.sparql.ast.cache;

import java.util.Set;

import org.openrdf.model.Graph;
import org.openrdf.query.GraphQueryResult;

import com.bigdata.rdf.internal.IV;

/**
 * A maintained cache for the DESCRIBE of RDF resources.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         TODO When has partitioned, each NSS instance (or DESCRIBE cache
 *         partition) should directly handle the describe of IVs that are
 *         directed to that partition.
 */
public interface IDescribeCache {
    
    /**
     * Destroy the cache.
     */
    void destroy();
    
    /**
     * Invalidate the identified cache entries.
     * 
     * @param ivs
     *            The {@link IV}s for the cache entries to be invalidated.
     */
    void invalidate(final Set<IV<?, ?>> ivs);

    /**
     * Lookup and return the cache entry.
     * 
     * @param iv
     *            The {@link IV}.
     * 
     * @return The cache entry -or- <code>null</code> if there is no entry for
     *         that {@link IV}.
     * 
     *         TODO Vector lookups for query. We will need to use that to
     *         support a star-join against the DESCRIBE cache (leveraging
     *         materialized joins).
     * 
     *         TODO Offer a stream oriented response. For a single {@link Graph}
     *         , that could be a {@link GraphQueryResult}. For vectored lookups,
     *         this might be a stream of binding sets (vectored lookups make
     *         sense primarily in star joins, so this would really be the
     *         signature for the star-join operator).
     */
    Graph lookup(final IV<?, ?> iv);
    
    /**
     * Insert/update the cache entry for an {@link IV}.
     * 
     * @param iv
     *            The {@link IV}.
     * @param g
     *            The {@link Graph} that describes that {@link IV}.
     * 
     *            TODO Vector inserts.
     * 
     *            TODO We probably need to include the timestamp of the database
     *            view from which the resource description was constructed.
     */
    void insert(final IV<?, ?> iv, final Graph g);

}
