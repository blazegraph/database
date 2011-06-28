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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop;

import java.util.Map;

import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Interface for evaluating operations producing chunks of elements (tuples
 * materialized from some index of a relation).
 * 
 * @see IAccessPath
 * @see IChunkedOrderedIterator
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractAccessPathOp<E> extends BOpBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends BOp.Annotations, BufferAnnotations {
        
    }

    /**
     * Required shallow copy constructor.
     * 
     * @param args
     * @param annotations
     */
    public AbstractAccessPathOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

    }

    /**
     * Required deep copy constructor.
     * 
     * @param op
     */
    public AbstractAccessPathOp(
            final AbstractAccessPathOp<E> op) {
        super(op);
    }

    /**
     * @see BufferAnnotations#CHUNK_CAPACITY
     */
    protected int getChunkCapacity() {
        
        return getProperty(Annotations.CHUNK_CAPACITY,
                Annotations.DEFAULT_CHUNK_CAPACITY);

    }

    /**
     * @see BufferAnnotations#CHUNK_OF_CHUNKS_CAPACITY
     */
    protected int getChunkOfChunksCapacity() {

        return getProperty(Annotations.CHUNK_OF_CHUNKS_CAPACITY,
                Annotations.DEFAULT_CHUNK_OF_CHUNKS_CAPACITY);

    }

//    protected int getFullyBufferedReadThreshold() {
//
//        return getProperty(Annotations.FULLY_BUFFERED_READ_THRESHOLD,
//                Annotations.DEFAULT_FULLY_BUFFERED_READ_THRESHOLD);
//
//    }

    /**
     * @see BufferAnnotations#CHUNK_TIMEOUT
     */
    protected long getChunkTimeout() {
        
        return getProperty(Annotations.CHUNK_TIMEOUT,
                Annotations.DEFAULT_CHUNK_TIMEOUT);
        
    }

}
