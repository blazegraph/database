package com.bigdata.rdf.sparql.ast.cache;

import java.util.Arrays;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Graph;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.htree.HTree;
import com.bigdata.io.SerializerUtil;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * An interface providing a maintained DESCRIBE cache for some
 * {@link AbstractTripleStore}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         FIXME MVCC VIEWS: The same integration issue also needs to be
 *         addressed for the {@link SparqlCache} for named solution sets.
 * 
 *         TODO Support hash partitioned and remove DESCRIBE cache instances.
 *         These will need access to a service that resolves {@link BigdataURI}
 *         s to {@link IV}s efficiently. That service can be exposed using the
 *         NSS or using jini/River.
 */
public class DescribeCache implements IDescribeCache {

    static private transient final Logger log = Logger
            .getLogger(SparqlCache.class);

    // /**
    // * The KB instance for which the cache is being maintained.
    // */
    // private final AbstractTripleStore tripleStore;

    /**
     * The cache. The keys are {@link IV}s. The values are the {@link Graph} s
     * describing those {@link IV}s.
     */
    private HTree cache;

    public DescribeCache(// final AbstractTripleStore tripleStore,
            final HTree cache) {

        // if(tripleStore == null)
        // throw new IllegalArgumentException();

        if (cache == null)
            throw new IllegalArgumentException();

        // this.tripleStore = tripleStore;

        this.cache = cache;

    }

    public void close() {

        this.cache.close();

    }

    /**
     * Return a thread-local instance.
     * 
     */
    private IKeyBuilder getKeyBuilder() {

        return cache.getIndexMetadata().getKeyBuilder();

    }

    /**
     * Returns the sort key for the URI.
     * 
     * @param uri
     *            The URI.
     * 
     * @return The sort key.
     */
    private byte[] iv2key(final IKeyBuilder keyBuilder, final IV<?, ?> iv) {

        if (keyBuilder == null)
            throw new IllegalArgumentException();

        if (iv == null)
            throw new IllegalArgumentException();

        keyBuilder.reset();

        return iv.encode(keyBuilder).getKey();

    }

    /**
     * {@inheritDoc}
     * 
     * TODO Compute the sketch and use an efficient representation for the
     * describe graph. The insert should be vectored, scalable, and page
     * oriented (blob stream API).
     * 
     * TODO If we explicit manage the raw records then we need to change how the
     * metadata is declared. We would have a fixed length value (the addr on the
     * backing store - either 4 or 8 bytes). We would also have to manage the
     * storage explicitly.
     */
    public void insert(final IV<?, ?> iv, final Graph g) {

        final byte[] key = iv2key(getKeyBuilder(), iv);

        final byte[] val = SerializerUtil.serialize(g);

        synchronized (cache) {

            cache.remove(key);

            cache.insert(key, val);

        }

    }

    public Graph lookup(final IV<?, ?> iv) {

        final byte[] key = iv2key(getKeyBuilder(), iv);

        final byte[] val = cache.lookupFirst(key);

        if (val == null)
            return null;

        final Graph x = (Graph) SerializerUtil.deserialize(val);

        return x;

    }

    /**
     * {@inheritDoc}
     * 
     * TODO If we explicitly manage raw records (rather than transparently) then
     * this MUST release the backing raw record for each cache entry.
     * 
     * TODO Invalidation should probably for lastCommitTime+1 (that is, anything
     * after the most current lastCommitTime). However, there is still a race
     * condition when something is invalidated while there is a concurrent
     * request to describe that thing. This can probably be solved by passing
     * along the timestamp that the DESCRIBE query is reading on to the
     * {@link DescribeCacheUpdater} and from there to the DESCRIBE cache. We
     * always should prefer the description of a resource associated with the
     * most current view of the database.
     */
    public void invalidate(final Set<IV<?, ?>> ivs) {

        if (ivs == null)
            throw new IllegalArgumentException();

        final int size = ivs.size();

        if (size == 0)
            return;

        final IV<?, ?>[] a = ivs.toArray(new IV[size]);

        // Sort 1st for better locality of updates.
        Arrays.sort(a);

        final IKeyBuilder keyBuilder = getKeyBuilder();

        synchronized (cache) {

            for (IV<?, ?> iv : a) {

                final byte[] key = iv2key(keyBuilder, iv);

                cache.remove(key);

            }

            if (log.isTraceEnabled())
                log.trace("Invalidated cache entries: n=" + size);

        }

    }

}