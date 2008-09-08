package com.bigdata.service.jini;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.bigdata.striterator.IAsynchronousIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IRemoteChunk;
import com.bigdata.striterator.IRemoteChunkedIterator;

/**
 * Wrapper for an {@link IChunkedOrderedIterator} exposing an interface suitable
 * for export as a proxy object using RMI to communicate back with itself and
 * pull data efficiently from the source iterator.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type of the elements visited by the source iterator.
 */
public class RemoteChunkedIterator<E> implements IRemoteChunkedIterator<E> {

    protected static final transient Logger log = Logger
            .getLogger(RemoteChunkedIterator.class);

    private final IChunkedOrderedIterator<E> sourceIterator;

//    /**
//     * The {@link Exporter} must be set when the proxy is exported. It will be
//     * used to unexport the proxy if the iterator is closed.
//     */
//    transient Exporter exporter = null;

    transient volatile boolean open = true;
    
    /**
     * 
     * @param sourceIterator
     *            The source iterator.
     */
    public RemoteChunkedIterator(IChunkedOrderedIterator<E> sourceIterator) {

        if (sourceIterator == null)
            throw new IllegalArgumentException();

        this.sourceIterator = sourceIterator;
        
        if (log.isDebugEnabled()) {

            log.debug("sourceIterator=" + sourceIterator);

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

            if (log.isInfoEnabled()) {

                log.info("Closing iterator.");

            }

            try {

                sourceIterator.close();

//                if (exporter != null) {
//
//                    try {
//
//                        /*
//                         * Make the object no longer available for RMI.
//                         * 
//                         * Can we do this during an RMI call from the client?
//                         * (That is what happens if the client closes the proxy
//                         * since this method is then invoked via RMI on the
//                         * server).
//                         */
//                        exporter.unexport(true/* force */);
//
//                        if (log.isInfoEnabled())
//                            log.info("Unexported proxy");
//
//                    } finally {
//
//                        exporter = null;
//
//                    }
//
//                }

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

            if (log.isInfoEnabled()) {

                log.info("nchunks=" + nchunks + " : source is exhausted");

            }

            chunk = new RemoteChunk<E>(true/* exhausted */, sourceIterator
                    .getKeyOrder(), null);

        } else {

            // @todo config timeout.
            final E[] a = sourceIterator instanceof IAsynchronousIterator//
                ? ((IAsynchronousIterator<E>) sourceIterator).nextChunk(//
                    1000,// minChunkSize
                    1000L, // timeout
                    TimeUnit.MILLISECONDS// unit
                    )//
                : sourceIterator.nextChunk()//
                ;

            final boolean sourceExhausted = !sourceIterator.hasNext();

            if (log.isInfoEnabled()) {

                log.info("nchunks=" + nchunks + ", elementsInChunk=" + a.length
                        + ", sourceExhausted=" + sourceExhausted);

            }

            chunk = new RemoteChunk<E>(sourceExhausted, sourceIterator
                    .getKeyOrder(), a);

        }

        nchunks++;

        return chunk;

    }

    private transient long nchunks = 0L;

}
