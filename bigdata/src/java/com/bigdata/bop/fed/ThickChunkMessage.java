/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Sep 9, 2010
 */

package com.bigdata.bop.fed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.engine.IChunkMessage;
import com.bigdata.bop.engine.IQueryClient;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;


/**
 * A thick version of this interface in which the chunk is sent inline with the
 * RMI message.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ThickChunkMessage<E> implements IChunkMessage<E>, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    final private IQueryClient queryController;

    final private long queryId;

    final private int bopId;
    
    final private int partitionId;

    final private int solutionCount;
    
    final private byte[] data;

    public IQueryClient getQueryController() {
        return queryController;
    }

    public long getQueryId() {
        return queryId;
    }

    public int getBOpId() {
        return bopId;
    }

    public int getPartitionId() {
        return partitionId;
    }
    
    public boolean isMaterialized() {
        return true;
    }

    public int getSolutionCount() {
        return solutionCount;
    }
    
    public int getBytesAvailable() {
        return data.length;
    }

    public String toString() {

        return getClass().getName() + "{queryId=" + queryId + ",bopId=" + bopId
                + ",partitionId=" + partitionId + ", solutionCount="
                + solutionCount + ", bytesAvailable=" + data.length + "}";

    }

    /**
     * 
     * @param queryController
     * @param queryId
     * @param bopId
     * @param partitionId
     * @param source
     */
    public ThickChunkMessage(final IQueryClient queryController,
            final long queryId, final int bopId, final int partitionId,
            final IBlockingBuffer<IBindingSet[]> source) {

        if (queryController == null)
            throw new IllegalArgumentException();

        if (source == null)
            throw new IllegalArgumentException();

        // do not send empty chunks.
        if (source.isEmpty())
            throw new IllegalArgumentException();

        this.queryController = queryController;
        
        this.queryId = queryId;

        this.bopId = bopId;

        this.partitionId = partitionId;

        /*
         * Format the data.
         */
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {

            final ObjectOutputStream oos = new ObjectOutputStream(baos);

            final IAsynchronousIterator<IBindingSet[]> itr = source.iterator();
            
            int solutionCount = 0;
            
            while(itr.hasNext()) {
            
                final IBindingSet[] a = itr.next();
                
                oos.writeObject(a);
                
                solutionCount += a.length;
                
            }
            
            oos.flush();
            baos.flush();

            this.data = baos.toByteArray();
        
            this.solutionCount = solutionCount;
            
        } catch (IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
    }

    public void materialize(FederatedRunningQuery runningQuery) {
        // NOP
    }

    public void release() {
        // NOP
    }
    
    /**
     * FIXME Provide in place decompression and read out of the binding sets.
     * This should be factored out into classes similar to IRaba and IRabaCoder.
     * This stuff should be generic so it can handle elements and binding sets
     * and bats, but there should be specific coders for handling binding sets
     * which leverages the known set of variables in play as of the operator
     * which generated those intermediate results.
     */
    public IAsynchronousIterator<E[]> iterator() {

        return new DeserializationIterator();

    }
    
    private class DeserializationIterator implements IAsynchronousIterator<E[]> {

        private volatile ObjectInputStream ois;
        private E[] current = null;
                
        public DeserializationIterator() {
        
            try {
                ois = new ObjectInputStream(new ByteArrayInputStream(data));
            
            } catch (IOException e) {
                
                throw new RuntimeException(e);
                
            }
            
        }

        public void close() {
            
            ois = null;
            
        }

        public boolean hasNext() {

            if (current != null)
                return true;
            
            try {
            
                current = (E[]) ois.readObject();
                
                return true;
                
            } catch (EOFException e) {
                
                return false;
                
            } catch (IOException e) {
                
                throw new RuntimeException(e);
                
            } catch (ClassNotFoundException e) {
                
                throw new RuntimeException(e);
                
            }
            
        }

        public E[] next() {

            if (!hasNext())
                throw new NoSuchElementException();
            
            final E[] tmp = current;
            
            current = null;
            
            return tmp;
            
        }

        public void remove() {
            
            throw new UnsupportedOperationException();
            
        }

        /*
         * Note: Asynchronous API is not implemented in a non-blocking manner
         * since all the data is in a byte[] and there is an expectation that
         * this interface will be excised from query processing soon.
         */

        public boolean hasNext(long timeout, TimeUnit unit)
                throws InterruptedException {
            return hasNext();
        }

        public boolean isExhausted() {
            return hasNext();
        }

        public E[] next(long timeout, TimeUnit unit)
                throws InterruptedException {
            return next();
        }

    }

}
