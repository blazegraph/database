package com.bigdata.rdf.lexicon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.openrdf.model.Value;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.BlobIV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * Batch resolve {@link BlobIV}s to RDF {@link Value}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 */
class BatchResolveBlobIVsTask implements Callable<Void> {

//    static private final transient Logger log = Logger
//            .getLogger(BatchResolveBlobIVsTask.class);

    private final ExecutorService service;
    private final IIndex ndx;
    private final Collection<BlobIV<?>> ivs;
    private final ConcurrentHashMap<IV<?,?>/* iv */, BigdataValue/* term */> ret;
    private final ITermCache<IV<?,?>, BigdataValue> termCache;
    private final BigdataValueFactory valueFactory;
    private final int MAX_CHUNK;

    public BatchResolveBlobIVsTask(
            final ExecutorService service,
            final IIndex ndx,
            final Collection<BlobIV<?>> ivs,
            final ConcurrentHashMap<IV<?, ?>/* iv */, BigdataValue/* term */> ret,
            final ITermCache<IV<?,?>, BigdataValue> termCache,
            final BigdataValueFactory valueFactory,
            final int chunkSize) {

        this.service = service;
        
        this.ndx = ndx;
        
        this.ivs = ivs;
        
        this.ret = ret;
        
        this.termCache = termCache;
        
        this.valueFactory = valueFactory;
        
        this.MAX_CHUNK = chunkSize;
        
    }

    public Void call() throws Exception {

        final int numNotFound = ivs.size();
        
        // An array of IVs that to be resolved against the index.
        final BlobIV<?>[] notFound = ivs.toArray(new BlobIV[numNotFound]);
        
        // Sort IVs into index order.
        Arrays.sort(notFound, 0, numNotFound);

        // Encode IVs as keys for the index.
        final byte[][] keys = new byte[numNotFound][];
        {

            final IKeyBuilder keyBuilder = KeyBuilder.newInstance();

            for (int i = 0; i < numNotFound; i++) {

                keys[i] = notFound[i].encode(keyBuilder.reset()).getKey();

            }
            
        }

        if (numNotFound < MAX_CHUNK) {

            /*
             * Resolve everything in one go.
             */

            new ResolveBlobsTask(ndx, 0/* fromIndex */,
                    numNotFound/* toIndex */, keys, notFound, ret,
                    termCache, valueFactory).call();

        } else {

            /*
             * Break it down into multiple chunks and resolve those chunks
             * in parallel.
             */

            // #of elements.
            final int N = numNotFound;
            // target maximum #of elements per chunk.
            final int M = MAX_CHUNK;
            // #of chunks
            final int nchunks = (int) Math.ceil((double) N / M);
            // #of elements per chunk, with any remainder in the last chunk.
            final int perChunk = N / nchunks;

            // System.err.println("N="+N+", M="+M+", nchunks="+nchunks+", perChunk="+perChunk);

            final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>(
                    nchunks);

            int fromIndex = 0;
            int remaining = numNotFound;

            for (int i = 0; i < nchunks; i++) {

                final boolean lastChunk = i + 1 == nchunks;

                final int chunkSize = lastChunk ? remaining : perChunk;

                final int toIndex = fromIndex + chunkSize;

                remaining -= chunkSize;

                // System.err.println("chunkSize=" + chunkSize
                // + ", fromIndex=" + fromIndex + ", toIndex="
                // + toIndex + ", remaining=" + remaining);

                tasks.add(new ResolveBlobsTask(ndx, fromIndex, toIndex,
                        keys, notFound, ret, termCache, valueFactory));

                fromIndex = toIndex;

            }

            try {

                // Run tasks.
                final List<Future<Void>> futures = service.invokeAll(tasks);

                // Check futures.
                for (Future<?> f : futures)
                    f.get();

            } catch (Exception e) {

                throw new RuntimeException(e);
                
            }

        }

//          final long elapsed = System.currentTimeMillis() - begin;
//          
//          if (log.isInfoEnabled())
//              log.info("resolved " + numNotFound + " terms in "
//                      + tasks.size() + " chunks and " + elapsed + "ms");
        
        // Done.
        return null;
        
    }
    
}