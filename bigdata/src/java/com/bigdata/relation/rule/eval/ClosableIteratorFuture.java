/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

*/
/*
 * Created on Jul 2, 2008
 */

package com.bigdata.relation.rule.eval;

import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.relation.accesspath.DelegateChunkedIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.accesspath.IClosableIterator;

/**
 * Class that knows how to cancel the {@link Future} that is writing on an
 * {@link IBlockingBuffer} when the {@link IChunkedOrderedIterator} draining
 * that {@link IBlockingBuffer} is {@link IClosableIterator#close()}ed.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ClosableIteratorFuture<E,F> extends DelegateChunkedIterator<E> {

    protected static final transient Logger log = Logger.getLogger(ClosableIteratorFuture.class);
    
    private final Future<F> future;
    
    public ClosableIteratorFuture(IBlockingBuffer<E> buffer, Future<F> future) {

        super(buffer.iterator());
        
        if (future == null)
            throw new IllegalArgumentException();
        
        this.future = future;
        
    }

    /**
     * Extended to cancel the {@link Future} that is writing on the
     * {@link IBlockingBuffer} when the iterator is {@link #close()}ed.
     */
    public void close() {

        if(!future.isDone()) {

            /*
             * If the query is still running and we close the iterator
             * then the query will block once the iterator fills up and
             * it will fail to progress. To avoid this, and to have the
             * query terminate eagerly if the client closes the
             * iterator, we cancel the future if it is not yet done.
             */
            
            if(log.isDebugEnabled()) {
                
                log.debug("will cancel future: "+future);
                
            }
            
            future.cancel(true/*mayInterruptIfRunning*/);
            
            if(log.isDebugEnabled()) {
                
                log.debug("did cancel future: "+future);
                
            }

        }

        // pass close() onto the delegate.
        super.close();
        
    }

}
