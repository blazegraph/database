/*

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Mar 15, 2007
 */

package com.bigdata.btree;

import java.io.Serializable;

import com.bigdata.service.ClientIndexView;
import com.bigdata.service.DataService;
import com.bigdata.service.IDataService;
import com.bigdata.sparse.SparseRowStore;

/**
 * An arbitrary procedure run against a single index.
 * <p>
 * There is a fairly direct correspondence between map / reduce processing and
 * the use of index procedures. The "map" phase corresponds to the
 * {@link IIndexProcedure} while the "reduce" phase corresponds to the
 * {@link IResultHandler}. The main difference is that index procedures operate
 * on ordered data. {@link IIndexProcedure}s MAY use
 * {@link IParallelizableIndexProcedure} to declare that they can be executed in
 * parallel when mapped across multiple index partitions - parallelization is
 * handled automatically by {@link ClientIndexView}. Otherwise it is assumed
 * that the procedure requires sequential execution as it is applied to the
 * relevant index partition(s) - again this is handled automatically.
 * <p>
 * There are basically three kinds of index procedures, each of which determines
 * where an index procedure will be mapped in a different manner:
 * <dl>
 * 
 * <dt>A single key, <code>byte[] key</code></dt>
 * 
 * <dd>Examples are when reading a logical row from a {@link SparseRowStore} or
 * performing a datum {@link ISimpleBTree#lookup(Object)}. These procedures are
 * always directed to a single index partition. Since they are never mapped
 * across index partitions, there is no "aggregation" phase. Likewise, there is
 * no {@link AbstractIndexProcedureConstructor} since the procedure instance is always
 * created directly by the application.</dd>
 * 
 * <dt>A key range, <code>byte[] toKey, byte[] fromKey</code></dt>
 * 
 * <dd>Examples are {@link IRangeQuery#rangeCount(byte[], byte[])} and
 * {@link IRangeQuery#rangeIterator(byte[], byte[]). Since the same data is sent
 * to each index partition there is no requirement for an object to construct
 * instances of these procedures. However, the procedures do need a means to
 * combine or aggregate the results from each index partition. See
 * {@link IKeyRangeIndexProcedure}.</dd>
 * 
 * <dt>A set of keys, <code>byte[][] keys</code></dt>
 * 
 * <dd>Examples are {@link BatchInsert}, {@link BatchLookup}, etc. For this
 * case the procedure is not only mapped across one or more index partitions,
 * but the procedure's data must be split such that each index partition is sent
 * only the data that is relevant to that index partition. See
 * {@link IKeyArrayIndexProcedure}. </dd>
 * 
 * </dl>
 * 
 * Note: this interface extends {@link Serializable}, however that provides
 * only for communicating state to the {@link IDataService}. If an instance of
 * this procedure will cross a network interface, then the implementation class
 * MUST be available to the {@link IDataService} on which it will execute. This
 * can be as simple as bundling the procedure into a JAR that is part of the
 * CLASSPATH used to start a {@link DataService} or you can use downloaded code
 * with the JINI codebase mechanism (<code>java.rmi.server.codebase</code>).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see IReadOnlyOperation
 */
public interface IIndexProcedure extends Serializable {

    /**
     * Run the procedure.
     * <p>
     * Note: Unisolated procedures have "auto-commit" ACID properties for a
     * local index only. In order for a distributed procedure to be ACID, the
     * procedure MUST be executed within a fully isolated transaction.
     * 
     * @param ndx
     *            The index.
     * 
     * @return The result, which is entirely defined by the procedure
     *         implementation and which MAY be <code>null</code>. In general,
     *         this MUST be {@link Serializable} since it may have to pass
     *         across a network interface.
     */
    public Object apply(IIndex ndx);

    /**
     * Interface for procedures that operation on a single index or index partition
     */
    public interface ISimpleIndexProcedure extends IIndexProcedure {
        
    }
    
    /**
     * Interface for procedures that are mapped across one or more index
     * partitions based on a key range (fromKey, toKey).  The keys are
     * interpreted as variable length unsigned byte[]s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface IKeyRangeIndexProcedure extends IIndexProcedure {
        
        /**
         * Return the lowest key that will be visited (inclusive). When
         * <code>null</code> there is no lower bound.
         */
        public byte[] getFromKey();

        /**
         * Return the first key that will not be visited (exclusive). When
         * <code>null</code> there is no upper bound.
         */
        public byte[] getToKey();

    }

    /**
     * Interface for procedures that are mapped across one or more index
     * partitions based on an array of keys. The keys are interpreted as
     * variable length unsigned byte[]s and MUST be in sorted order. The
     * {@link ClientIndexView} will transparently break down the procedure into
     * one procedure per index partition based on the index partitions spanned
     * by the keys.
     * <p>
     * Note: Implementations of this interface MUST declare an
     * {@link AbstractIndexProcedureConstructor} that will be used to create the
     * instances of the procedure mapped onto the index partitions.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface IKeyArrayIndexProcedure extends IIndexProcedure {

        /**
         * The #of keys/tuples
         */
        public int getKeyCount();
        
        /**
         * Return the key at the given index.
         * 
         * @param i
         *            The index (origin zero).
         * 
         * @return The key at that index.
         */
        public byte[] getKey(int i);
        
        /**
         * Return the value at the given index.
         * 
         * @param i
         *            The index (origin zero).
         * 
         * @return The value at that index.
         */
        public byte[] getValue(int i);
        
    }

}
