package com.bigdata.service.jini;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.bigdata.io.ISerializer;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.striterator.IKeyOrder;
import com.bigdata.striterator.IRemoteChunk;
import com.bigdata.striterator.IRemoteChunkedIterator;

/**
 * Wrapper for an {@link IAsynchronousIterator} exposing an interface suitable
 * for export as a proxy object using RMI to communicate back with itself and
 * pull data efficiently from the source iterator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type of the component elements visited by the source
 *            iterator.
 */
public class RemoteChunkedIterator<E> implements IRemoteChunkedIterator<E> {

    protected static final transient Logger log = Logger
            .getLogger(RemoteChunkedIterator.class);

    protected static final boolean INFO = log.isInfoEnabled();

    protected static final boolean DEBUG = log.isDebugEnabled();
    
    private final IAsynchronousIterator<E[]> sourceIterator;

    private final ISerializer<E[]> serializer;
    
    private final IKeyOrder<E> keyOrder;
    
    transient volatile boolean open = true;
    
    /**
     * 
     * @param sourceIterator
     *            The source iterator.
     * @param serializer
     *            The object that will be used to (de-)serialize the elements.
     * @param keyOrder
     *            The natural order of the visited elements if known and
     *            otherwise <code>null</code>.
     */
    public RemoteChunkedIterator(IAsynchronousIterator<E[]> sourceIterator,
            ISerializer<E[]> serializer,
            IKeyOrder<E> keyOrder) {

        if (sourceIterator == null)
            throw new IllegalArgumentException();

        if (serializer == null)
            throw new IllegalArgumentException();

        this.sourceIterator = sourceIterator;

        this.serializer = serializer;
        
        this.keyOrder = keyOrder;
        
        if (DEBUG) {

            log.debug("sourceIterator=" + sourceIterator + ", serializer="
                    + serializer);

        }

    }

    /**
     * Note: since [keepAlive := false], this appears to be sufficient to force
     * the proxy to be unexported and force the source iterator to be closed.
     */
    protected void finalize() throws Throwable {

        if(open) {

            log.warn("Closing iterator");

            close();
            
        }

        super.finalize();

    }

    /**
     * Close the source iterator.
     */
    public void close() throws IOException {

        if (open) {

            if (INFO) {

                log.info("Closing iterator.");

            }

            try {

                sourceIterator.close();

            } finally {

                open = false;

            }

        }

    }

    /**
     * Return the next {@link IRemoteChunk} from the source iterator. If it
     * is exhausted then return an {@link IRemoteChunk} which indicates that
     * no more results are available.
     */
    public IRemoteChunk<E> nextChunk() throws IOException {

        final IRemoteChunk<E> chunk;

        if (!sourceIterator.hasNext()) {

            if (INFO) {

                log.info("nchunks=" + nchunks + " : source is exhausted");

            }

            chunk = new RemoteChunk<E>(true/* exhausted */, serializer,
                    keyOrder, null);

        } else {

//            // @todo config timeout.
//            final E[] a = sourceIterator instanceof IAsynchronousIterator//
//                ? ((IAsynchronousIterator<E>) sourceIterator).nextChunk(//
//                    1000,// minChunkSize
//                    1000L, // timeout
//                    TimeUnit.MILLISECONDS// unit
//                    )//
//                : sourceIterator.nextChunk()//
//                ;

            final E[] a = sourceIterator.next();
            
            /*
             * FIXME This forces us to wait until there is another chunk
             * materialized and waiting in the BlockingBuffer's queue or until
             * the future is done while the queue is empty. That means that we
             * really wait for two chunks to be ready rather than returning when
             * the first chunk is available.
             * 
             * Change the RemoteChunk semantics to have the flag indicate only
             * whether the iterator is KNOWN to be exhausted. When not known to
             * be exhausted the caller must verify whether or not additional
             * chunks are available by making another RMI request.
             */
            final boolean sourceExhausted = !sourceIterator.hasNext();

            if (INFO) {

                log.info("nchunks=" + nchunks + ", elementsInChunk=" + a.length
                        + ", sourceExhausted=" + sourceExhausted);

            }

            chunk = new RemoteChunk<E>(sourceExhausted, serializer, keyOrder, a);

        }

        nchunks++;

        return chunk;

    }

    private transient long nchunks = 0L;

}
