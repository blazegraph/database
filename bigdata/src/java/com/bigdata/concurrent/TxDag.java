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
package com.bigdata.concurrent;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

/**
 * <p>
 * Directed Acyclic Graph (DAG) for detecting and preventing deadlocks in a
 * concurrent programming system. The algorithm takes advantage of certain
 * characteristics of the deadlock detection problem for concurrent transactions
 * and provides a reasonable cost solution for that domain. The design uses a
 * boolean matrix W to code the edges in the WAITS_FOR graph and an integer
 * matrix M to code the the number of different paths between two vertices.
 * Operations that insert one or more edges are atomic -- if a deadlock would
 * result, then the state of the DAG is NOT changed (a deadlock is detected when
 * there is a non-zero path count in the diagonal of W). The cost of the
 * algorithm is less than <code>O(n^^2)</code> and is suitable for systems
 * with a multi-programming level of 100s of concurrent transactions.
 * </p>
 * <p>
 * This implementation is based on the online algorithm for deadlock detection
 * in Section 5, page 86 of:
 * <code>Bayer, R. 1976. Integrity, Concurrency, and Recovery in Databases. In
 * Proceedings of the Proceedings of the 1st European Cooperation in informatics
 * on ECI Conference 1976 (August 09 - 12, 1976). K. Samelson, Ed. Lecture Notes
 * In Computer Science, vol. 44. Springer-Verlag, London, 79-106.
 * </code>
 * See the <a href="http://portal.acm.org/citation.cfm?id=647864.736638">
 * citation </a> online.
 * </p>
 * <p>
 * Given that <code>w</code> is the directed acyclic graph of
 * <code>WAITS_FOR</code> relation among the concurrent transactions.
 * <code>w+</code> is the transitive closure of <code>w</code>. The online
 * algorithm solves the problem:
 * </p>
 * 
 * <pre>
 * Given w, w+,              calculate
 *       w', w'+             where
 *       w' := w U {(ti,tj)} iff inserting an edge, or
 *       w' := w / {(ti,tj)} iff removing an edge.
 * </pre>
 * 
 * <p>
 * The approach defines a matrix <code>M[ t, u ]</code> whose cells are the
 * number of different paths from <code>t</code> to <code>u</code>. A
 * deadlock is identified if the update algorithm for <code>M</code> computes
 * a non-zero value for the diagonal.
 * </p>
 * <p>
 * The update rules for M are as follows. "+/-" should be interpreted as "+" iff
 * an edge is being added and "-" iff an edge is being removed. The "." operator
 * is scalar multiplication.
 * <ul>
 * <li>M[s,v] := M[s,v] +/- M[s,t] . M[u,v]; s!=t; u!=v</li>
 * <li>M[s,u] := M[s,u] +/- M[s,t]; s!=t</li>
 * <li>M[t,v] := M[t,v] +/- M[u,v]; u!=v</li>
 * <li>M[t,u] := M[t,u] +/- 1</li>
 * </ul>
 * Updates are made tentative using a secondary matrix, M2. The update is
 * applied to M2. If a deadlock would result, then the original matrix is not
 * modified. Otherwise the original matrix is replaced by M2.
 * </p>
 * <p>
 * The public interface is defined in terms of arbitrary objects designated by
 * the application as "transactions". The DAG is provisioned for a maximum
 * multi-programming level, which is used to dimension the internal matrices.
 * The choosen multi-programming level should correspond to the maximum multi-
 * programming level permitted, i.e., to the #of concurrent transactions which
 * may execute before subsequent requests for a new transaction are queued.
 * Internally the "transaction" objects are mapped using their hash code onto
 * pre-defined {@link Integer}s corresponding to indices in [0:n-1], where n is
 * the provisioned multi-programming level.
 * </p>
 * <h4>Usage notes</h4>
 * <p>
 * This class is designed to be used internally by a class modeling a
 * {@link ResourceQueue}. Edges are added when a transaction must <em>wait</em>
 * for a resource on one or more transactions in the granted group for that
 * resource queue. Transactions are implicitly declared as they are referenced
 * when adding edges. The general case is that there are N transactions in the
 * granted group for some resource, so
 * {@link #addEdges(Object blocked, Object[] running)} would be used to indicate
 * that a transaction must wait on the granted group.
 * </p>
 * <p>
 * A transaction in a granted group is guarenteed to be running and hence not
 * waiting on any other transaction(s). When a transaction releases a lock, the
 * {@link ResourceQueue} automatically invokes
 * {@link #removeEdges(Object tx, boolean waiting)} with
 * <code>waiting == false</code> in order to remove all WAITS_FOR
 * relationships whose target is that transaction. (The
 * {@link #removeEdges(Object, boolean)} method is optimized for the case when
 * it is known that a transaction is not waiting for any other transaction.)
 * </p>
 * <p>
 * The integration layer MUST explicitly invoke
 * {@link #removeEdges(Object, boolean)} whenever a transaction commits or
 * aborts in order to remove all WAITS_FOR relationships involving that
 * transaction. Failure to do this will result in false reporting of deadlocks
 * since the transaction is still "on the books". The integration layer should
 * specify <code>waiting == false</code> iff it knows that the transaction was
 * NOT blocked. For example, a transaction which completes normally is never
 * blocked. However, if a decision is made to abort a blocked transaction, e.g.,
 * owing to a timeout or external directive, then the caller MUST specify
 * <code>waiting == true</code> and a less efficient technique will be used to
 * remove all edges involving the specificed transaction.
 * </p>
 * 
 * @todo This implementation does not help the application to decide the minimum
 *       "cost" set of transactions which would result in an acyclic graph if
 *       their WAITS_FOR relationships (edges) were removed from the graph. This
 *       could probably be achieved by an analysis of the path count matrix in
 *       which the deadlock was detected combined with information about the
 *       sunk cost of each transaction.
 * 
 * @todo The use of DAGs for detecting and breaking deadlocks in support of
 *       concurrent programming may require interaction with the locking
 *       protocol. For example, a transaction requesting a lock which would
 *       result in a deadlock may be moved up in the request queue for a lock if
 *       that would resolve the deadlock.
 * 
 * @todo Consider requiring explicitly registration of transactions. This is
 *       parallel to the requirement for explicitly removal of transactions
 *       using {@link #removeEdges(Object, boolean).
 * 
 * @todo This class should probably be unsynchronized and should place the
 *       burden for synchronization on the caller.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */

public class TxDag {

	/**
	 * Logger for this class.
	 */
    protected static final Logger log = Logger.getLogger(TxDag.class);

    protected static final boolean INFO = log.isInfoEnabled();
    
    protected static final boolean DEBUG = log.isDebugEnabled();

    /**
     * The maximum multi-programming level supported (from the constructor).
     */
    private final int _capacity;
    
    /**
     * The asserted edges in the directed acyclic graph. W[u,v] is true iff u
     * WAITS_FOR v.
     */
    private final boolean[][] W;
    
    /**
     * The #of different paths from u to v in {@link #W}.
     */
    private final int[][] M;
    
    /**
     * A scratch buffer used to make conditional updates of M.
     * 
     * @see #backup()
     */
    private final int[][] M1;
    
    /**
     * The #of inbound edges for each transaction index.
     */
    final int[] inbound;
    
    /**
     * The #of outbound edges for each transaction index.
     */
    final int[] outbound;

    /**
	 * An array of the application transaction objects in order by the indices
	 * as assigned by {@link #lookup(Object, boolean)}. Entries in this array
	 * are cleared (to <code>null</code>) when a vertex is removed from the
	 * graph by {@link #releaseVertex(Object)}.
	 */
    final Object[] transactions;
    
    /**
	 * Caches the results of the last {@link #getOrder()} call.
	 */
    private int[] _order = null;
    
    /**
     * An empty int[] used for order[] when the graph is empty.
     */
    private final int[] EMPTY = new int[]{};
    
    /**
	 * This field controls whether or not the result of {@link #getOrder()} and
	 * {@link #getOrder(int, int)} are sorted. Sorting is not required for
	 * correctness, but sorting may make it easier to follow the behavior of the
	 * algorithm. The default is <code>false</code>.
	 */
    static public boolean sortOrder = false;
    
    /**
     * This field controls whether or not the order[] is cloned and then sorted
     * by {@link #toString()}.  The default is <code>true</code>.
     */
    static public boolean sortOrderForDisplay = true;
    
    /**
	 * This field controls whether or not the result of {@link #getOrder()} is
	 * cached. Caching is enabled by default but may be disabled for debugging.
	 */
    static public boolean cacheOrder = false;
    
    /**
     * The constant used by {@link #lookup(Object, boolean)} to indicate that
     * the named vertex was not found in the DAG (<code>-1</code>).
     */
    static public final int UNKNOWN = -1;
    
    /**
     * A list containing {@link Integer} indices available to be assigned to a
     * new transaction. When this list is empty, then the maximum #of
     * transactions are running concurrently.  Entries are removed from the
     * list when they are assigned to a transaction. Entries are returned to
     * the list when a transaction is complete (abort or commit).
     */
    private final List<Integer> indices = new LinkedList<Integer>();

    /**
     * Mapping from the application "transaction" object to the {@link Integer}
     * index assigned to that transaction.
     * 
     * @see #indices
     */
    private final Map<Object,Integer> mapping = new HashMap<Object, Integer>();
    
    /**
     * The maximum multi-programming level supported (from the constructor).
     * 
     * @return The maximum vertex count.
     * 
     * @see #size()
     */
    
    public int capacity() {
    	// Note: synchronization is not required - final data.
        return _capacity;
    }
    
    /**
     * The current multi-programming level. This is simply the #of distinct
     * transactions in the WAITS_FOR relationship or alternatively the #of
     * vertices in the DAG.
     * 
     * @return The vertex count.
     * 
     * @see #capacity()
     */

    synchronized public int size() {
        return mapping.size();
    }

    /**
     * Return <code>true</code> iff adding another transaction would exceed
     * the configured multi-programming capacity.
     */
    synchronized public boolean isFull() {

        return size() == _capacity;

    }
    
    /**
     * Constructor.
     * 
     * @param capacity
     *            The multi-programming level. This is the maximum number of
     *            concurrent transactions permitted by the application.
     * 
     * @param IllegalArgumentException
     *            If <code>n &lt; 2 </code> (the minimum value for
     *            concurrency).
     */

	public TxDag( int capacity ) {
        super();
        final int n = capacity; // rename variable.
        if( n < 2 ) {
            throw new IllegalArgumentException();
        }
        this._capacity = n;
        W = new boolean[ n ][ n ]; // edges.
        M = new int[ n ][ n ]; // path count matrix.
        M1 = new int[ n ][ n ]; // backup of path count data for restore when deadlock results.
        inbound = new int[ n ]; // #of inbound edges for each transaction index. 
        outbound = new int[ n ]; // #of outbound edges for each transaction index.
        transactions = new Object[ n ]; // application's transaction objects in index order.
        for( int i=0; i<n; i++ ) {
            indices.add( Integer.valueOf( i ) );
            inbound[ i ] = 0;
            outbound[ i ] = 0;
            transactions[ i ] = null;
            for( int j=0; j<n; j++ ) {
                W[ i ][ j ] = false;
                M[ i ][ j ] = 0;
                M1[ i ][ j ] = 0;
            }
        }
    }

    /**
     * Lookup index assigned to transaction object.
     * 
     * @param tx
     *            The transaction object.
     * 
     * @param insert
     *            When true an index will be assigned iff none is currently
     *            assigned to that transaction object.
     * 
     * @return The index assigned to the transaction object or {@link #UNKNOWN}
     *         iff there is no index assigned to <i>tx </i>.
     * 
     * @exception IllegalArgumentException
     *                If the <code>tx == null</code>.
     * @exception IllegalStateException
     *                If the transaction is not associated with a vertex of the
     *                DAG, <code>insert == true</code>, and the capacity
     *                would be exceeded if this transaction was added.
     */
    synchronized int lookup( final Object tx, final boolean insert )
    {
        
        if( tx == null ) {
            
            throw new IllegalArgumentException("transaction object is null");
            
        }
        
        Integer index = (Integer) mapping.get( tx );
        
        if (index == null) {

            if (insert) {

            	final int capacity = capacity();
            	
            	final int nvertices = mapping.size();
            	
                if( nvertices == capacity ) {

                    throw new MultiprogrammingCapacityExceededException(
                            "capacity=" + capacity + ", nvertices=" + nvertices);

                }

                /*
                 * Assign the transaction a free index. Throws
                 * IndexOutOfBoundsException if there is no free index
                 * available.
                 */

                index = (Integer) indices.remove(0);

//                if (index == null) {
//
//                    throw new AssertionError("no free index to assign?");
//
//                }

                mapping.put(tx, index); // add transaction to mapping.

                final int ndx = index.intValue();
                
                if( transactions[ ndx ] != null ) {
                	
                	throw new AssertionError();
                	
                }
                
                transactions[ ndx ] = tx;
                
//                resetOrder(); // reset order[] cache.

            } else {
                
                // Not found, insert is false.
            	
                return -1;
                
            }

        }
        
        // Found / inserted.
        return index.intValue();

    }
    
    /**
     * Releases the vertex by (a) removing it from the {@link #mapping} and (b)
     * updating the list of available {@link #indices}.
     * 
     * @param tx
     * 
     * @return true iff the vertex was known.
     * 
     * FIXME Should it be an error if there is an edge remaining for that
     * vertex? if we do not detect this condition then is is possible that
     * uncleared edges will remainin in the WAITS_FOR graph and will interfere
     * with reuse of the recycled index?
     */
    synchronized public boolean releaseVertex( final Object tx )
    {
    	
    	final Integer index = (Integer) mapping.remove( tx );
    	
    	if( index == null ) {
    	
//    		throw new IllegalArgumentException("tx="+tx);
    	    if (INFO)
                log.info("Not a vertex: " + tx);
            
            return false;
    		
    	}
    	
    	indices.add( index ); // return to list of free indices.
    	
    	final int ndx = index.intValue();
    	
    	if( transactions[ ndx ] == null ) {
    		
    		throw new AssertionError();
    		
    	}
    	
    	transactions[ ndx ] = null;
    	
//    	resetOrder(); // invalidate the order[] cache.
    	
        return true;
        
    }
    
    /**
     * Return the #of different paths from u to v.
     * 
     * @param u
     *            The index assigned to some transaction.
     * @param v
     *            The index assigned to some other transaction.
     * 
     * @return The #of different paths from u to v.
     */
    
    final synchronized int getPathCount( final int u, final int v )
    {
        
        return M[ u ][ v ];
        
    }

    /**
	 * Return a copy of the entire path count matrix.
	 * 
	 * @return A copy of the array M whose dimensions are [capacity][capacity].
	 * 
	 * @see #capacity()
	 */

    final synchronized int[][] getPathCountMatrix()
    {
        
        return (int[][])M.clone();
        
    }
    
    /**
	 * Add an edge to the DAG. The edge has the semantics
	 * <code> blocked -&gt; running[ i ]</code>, i.e., the <i>blocked </i>
	 * transaction <em>WAITS_FOR</em> the <i>running </i> transaction.
	 * 
	 * @param blocked
	 *            A transaction. If the transaction is not already registered as
	 *            a vertex of the graph, then it is implicitly declared by this
	 *            method.  See {@link #lookup(Object, boolean)}.
	 * @param running
	 *            A different transaction. If the transaction is not already
	 *            registered as a vertex of the graph, then it is implicitly
	 *            declared by this method.  See {@link #lookup(Object, boolean)}.
	 * 
	 * @exception IllegalArgumentException
	 *                If either argument is <code>null</code>.
	 * @exception IllegalArgumentException
	 *                If the same transaction is specified for both arguments.
	 * @exception IllegalStateException
	 *                If the described edge already exists.
	 * @exception DeadlockException
	 *                If adding the edge to the DAG would result in a cycle. The
	 *                state of the DAG is unchanged if this exception is thrown.
	 */
    synchronized public void addEdge( final Object blocked, final Object running )
    	throws DeadlockException
    {
        // verify arguments some more.
        if( running == blocked ) {
            throw new IllegalArgumentException("may not wait for self");
        }
        final int dst = lookup( running, true );
        final int src = lookup( blocked, true );
        if( src == dst ) {
            throw new IllegalArgumentException("may not wait for self.");
        }
        if( W[src][dst] ) {
            throw new IllegalStateException("edge exists");
        }
        /*
		 * Make a backup of the in-use cells of M.  Apply changes directly to M.
		 * If a deadlock results, then restore M from the back.  (This approach
		 * presumes that deadlocks are less likely than success.)
		 */
        if( DEBUG ) {
        	log.debug(toString());
        }
        final int[] order = getOrder();
        backup(order);
		try {
	        if( ! updateClosure( src, dst, true ) ) {
				/*
				 * Deadlock - rollback tentative change to M.
				 */
                log.warn("Deadlock");
				restore(order);
				if( DEBUG ) {
					log.debug(toString());
				}
				throw new DeadlockException("deadlock");
			}
		} catch (DeadlockException ex) {
			/*
			 * Deadlock - already handled above, just rethrow the exception.
			 */
			throw ex;
		} catch (Throwable t) {
			/*
			 * Unexpected exception - rollback the tentative change, log an
			 * error message, and then throw a wrapped exception.
			 */
			log.error(t);
			restore(order);
			throw new RuntimeException(t);
		}
		// No deadlock - update W.
		W[src][dst] = true;
		// Update outbound and inbound counters.
		outbound[src]++;
		inbound[dst]++;
		// If either counter was zero (and is now one), then reset the
		// order[] cache.
		if( outbound[src] == 1 || inbound[dst] == 1 ) {
			resetOrder();
		}
		if( DEBUG ) {
			log.debug(toString());
		}
    }

    /**
	 * Creates a BFIM (before image) of the in-use cells from M. A per-instance
	 * scratch buffer is used to store the BFIM. Only in-use cells are actually
	 * written on the BFIM.
	 * <p>
	 * This method is invoked in two contexts. One in which a single edge is
	 * being added and another in which multiple edges are being added. In
	 * either case the caller MUST make a copy of the path count matrix and MUST
	 * NOT update W until the operation has succeeded without deadlock.
	 */
    
    synchronized final void backup( final int[] order )
    {
    	final int n = order.length;
    	for( int i=0; i<n; i++ ) {
    		final int oi = order[i];
    		for( int j=0; j<n; j++ ) {
    			final int oj = order[j];
    			M1[oi][oj] = M[oi][oj]; 
    		}
    	}
    }

    /**
	 * Restore M when a deadlock resulted.
	 * 
	 * @param order
	 *            This <em>MUST</em> be the same order[] passed to
	 *            {@link #backup(int[] order)}. If a different order[] is used
	 *            then the wrong values will be restored.
	 */
    
    synchronized final void restore( final int[] order )
    {
    	final int n = order.length;
    	for( int i=0; i<n; i++ ) {
    		final int oi = order[i];
    		for( int j=0; j<n; j++ ) {
    			final int oj = order[j];
				M[oi][oj] = M1[oi][oj]; 
    		}
    	}
    }

    /**
	 * Add or remove an edge <code>src WAITS_FOR dst</code>and update the
	 * closure of the WAITS_FOR graph.
	 * 
	 * @param t
	 *            The index associated with a transaction (src WAITS_FOR dst).
	 * @param u
	 *            The index associated with another transaction.
	 * @param insert
	 *            True iff an edge is being inserted and false iff an edge is
	 *            being removed.
	 * 
	 * @return true iff no deadlock was created, in which case <i>M </i> holds
	 *         the new state and t -&gt; u should be added to the WAITS_FOR
	 *         graph. false iff a deadlock resulted, in which case
	 *         <code>M[t,t] &gt; 0</code> for some t, indicating the presence
	 *         of a deadlock and <i>M </i> should be discarded. Note that
	 *         deadlock never results if <code>insert == false </code>.
	 * 
	 * @exception ArithmeticException
	 *                If the path count for any cell of the matrix would
	 *                overflow an <code>int</code>.
	 * 
	 * @see #backup(int[] order)
	 * @see #restore(int[] order)
	 */
    
    final synchronized boolean updateClosure(final int t, final int u,
			final boolean insert)
    {
    	/*
		 * Note: Path counts can grow large quite quickly since they are
		 * multiplicative. If you observe {@link ArithmeticException} being
		 * thrown from this method under reasonable use cases then change the
		 * definition of M from int[][] to long[][] and update the code below to
		 * test for exceeding {@link Long#MAX_VALUE} rather than
		 * {@link Integer#MAX_VALUE}. This will require changing parts of the
		 * package private API, but it should not effect the public API.
		 */
        /*
		 * Note: t, u are already indices into M. order[] is used to map the
		 * other transactions into indices within M, but is NOT used with t and
		 * u. Indices of M not found in order[] are unused at the time this
		 * method is invoked. os := order[s]. ov := order[v].
		 * 
		 * Note: DO NOT write code like: for( int s=0, os=order[s]; ... ) -- it
		 * produces the wrong behavior.
		 */
    	final int[] order = ( insert ? getOrder(t,u) : getOrder() );
    	final int n = order.length;
        if( DEBUG ) {
        	log.debug("W:: t("+t+") -> u("+u+"), insert="+insert+", size="+n);
        }
        final int max = Integer.MAX_VALUE;
        for( int s=0; s<n; s++ ) {
            final int os = order[ s ];
            if( os == t ) continue;
            for( int v=0; v<n; v++ ) {
                final int ov = order[ v ];
                if( ov == u ) continue;
                // M[s,v] := M[s,v] +/- M[s,t] . M[u,v]; s!=t; u!=v
                if( DEBUG ) {
                	log.debug("M[s="+os+",v="+ov+"] := "+
                			  "M[s="+os+",v="+ov+"]("+M[os][ov]+") +/- "+
                			  "M[s="+os+",t="+t+"]("+M[os][t]+") . "+
                			  "M[u="+u+",v="+ov+"]("+M[u][ov]+")"
                          	);
                }
                if( insert ) {
                    long val = M[os][ov] + ( M[os][t] * M[u][ov] );
                    if( val > max ) throw new ArithmeticException("overflow");
                    M[os][ov] = (int)val;
                } else {
                    M[os][ov] -= M[os][t] * M[u][ov];
                }
            }
            // M[s,u] := M[s,u] +/- M[s,t]; s!=t
            if( DEBUG ) {
            log.debug("M[s="+os+",u="+u+"] := "+
                      "M[s="+os+",u="+u+"]("+M[os][u]+") +/- "+
                      "M[s="+os+",t="+t+"]("+M[os][t]+")"
                      );
            }
            if( insert ) {
                long val = M[os][u] + M[os][t];
                if( val > max ) throw new ArithmeticException("overflow");
                M[os][u] = (int) val;
            } else {
                M[os][u] -= M[os][t];                
            }
        }
        for( int v=0; v<n; v++ ) {
            final int ov = order[ v ];
            if( ov == u ) continue;
            // M[t,v] := M[t,v] +/- M[u,v]; u!=v
            if( DEBUG ) {
            log.debug("M[t="+t+",v="+ov+"] := "+
                      "M[t="+t+",v="+ov+"]("+M[t][ov]+") +/- "+
                      "M[u="+u+",v="+ov+"]("+M[t][ov]+")"
                      );
            }
            if( insert ) {
            	long val = M[t][ov] + M[u][ov];
            	if( val > max ) throw new ArithmeticException("overflow");
                M[t][ov] = (int) val;
            } else {
                M[t][ov] -= M[u][ov];
            }
        }
        // M[t,u] := M[t,u] +/- 1
        if( DEBUG ) {
        log.debug("M[t="+t+",u="+u+"] := "+
                  "M[t="+t+",u="+u+"]("+M[t][u]+") +/- 1"
        	  );
        }
        if( insert ) {
        	if( M[t][u] == max ) throw new ArithmeticException("overflow");
            M[t][u] += 1;
        } else {            
            M[t][u] -= 1;
        }
        // check for deadlock.
        for( int s=0; s<n; s++ ) {
        	final int os = order[s];
            if( M[os][os] > 0 ) {
            	if( DEBUG ) {
            		log.debug("deadlock: M["+os+","+os+"]="+M[os][os]);
            	}
                return false;
            }
        }
        return true;
    }
    
    /**
	 * Returns a representation of the state of the graph suitable for debugging
	 * the algorithm.
	 */

    synchronized public String toString()
    {
    	final int[] order;
    	if( sortOrderForDisplay) {
    		/*
			 * Copy and then sort order[]. We make a copy since this would
			 * otherwise sort the cached order[], which would have side effects
			 * that I want to avoid when debugging.
			 */
    		order = (int[]) getOrder().clone();
    		Arrays.sort(order); // sort indices for display purposes.
    	} else {
    		// Get order[] -- may be cached.
    		order = getOrder();
    	}
        StringBuffer sb = new StringBuffer();
//        final int n = size();
        sb.append("TxDag::\ncapacity="+capacity()+", size="+size()+"\n");
        // get the in-use transaction indices into W and M.
        sb.append( "index\t#in\t#out\n");
        for( int i=0; i<order.length; i++ ) {
        	final int oi = order[i];
            sb.append(""+order[i]+"\t"+inbound[oi]+"\t"+outbound[oi]+"\n");
        }
        /*
		 * Matrix W. Note that W is not updated by updateClosure(), but only be
		 * the routines which call that method. Therefore you will not see the
		 * edges displayed unless you are operating at that level in the API
		 * (some of the test cases do not, so if you are wondering why you are
		 * not seeing the edges listed, that is probably why).
		 */
        for( int i=0; i<order.length; i++ ) {
        	final int oi = order[i];
            for( int j=0; j<order.length; j++ ) {
            	final int oj = order[j];
                if(W[oi][oj]) {
                    sb.append("\t"+transactions[oi]+" -> "+transactions[oj]+"\n");
                }
            }
        }
        /*
         * Matrix M.
         */
        // column headings.
        sb.append("index");
        for( int j=0; j<order.length; j++ ) {
            sb.append("\t"+order[j]);
        }
        sb.append("\n");
        // matrix contents.
        boolean deadlock = false;
        for( int i=0; i<order.length; i++ ) {
        	final int oi = order[i];
            sb.append(""+oi); // row heading
            for( int j=0; j<order.length; j++ ) {
            	final int oj = order[j];
                final int count = M[oi][oj];
                if( count != 0 ) {
                    sb.append("\t"+count);
                } else {
                    sb.append("\t-");
                }
                if( oi == oj && count > 0 ) {
                	deadlock = true;
                }
            }
            sb.append("\t"+transactions[oi]+"\n"); // row trailer
        }
        // column footers
        for( int j=0; j<order.length; j++ ) {
            sb.append("\t"+transactions[order[j]]);
        }
        sb.append("\n");
        sb.append("deadlock="+deadlock+"\n");
        return sb.toString();
    }
    
    /**
	 * <p>
	 * Package private method returns a dense array containing a copy of the
	 * in-use transaction indices that participate in at least one edge.
	 * Transactions which have been declared to the {@link TxDag} but which have
	 * neither inbound nor outbound edges are not reported. Such transactions
	 * are neither waiting for other transactions nor being waited on by other
	 * transactions and do not participate when computing the
	 * {@link #updateClosure(int, int, boolean) closure} of W. By using only
	 * those transactions that participate in at least one edge we reduce the
	 * complexity of the closure update algorithm to an average complexity of
	 * <code>O((|W+|/n)^^2)</code>, where |W+| is the length of the returned
	 * array.
	 * </p>
	 * 
	 * @return A dense array of the transaction indices that also participate in
	 *         at least one edge. The indices may be present in any order and
	 *         the order may change from invocation to invocation -- even when
	 *         the state of the graph has not changed.
	 * 
	 * @see #resetOrder()
	 * @see #getOrder( int t, int u)
	 */

    synchronized int[] getOrder()
    {
    	if ( cacheOrder && _order != null) {
    		// return cached value.
			return _order;
		}
    	// #of "in-use" transactions.
		final int n = size();
		if (n == 0) {
			_order = EMPTY;
			return _order;
		}
		/*
		 * Compute #of transactions actually used in at least one edge. We do
		 * this by scanning the mapping and then verifying that each index in
		 * turn serves as either the source or the target for at least one edge.
		 */
		final int[] tmp = new int[n];
		int nnzero = 0;
		final Iterator itr = mapping.values().iterator();
		while (itr.hasNext()) {
			final int index = ((Integer) itr.next()).intValue();
			if( inbound[index]>0 || outbound[index]>0 ) {
				tmp[nnzero++] = index;
			}
		}
		if (nnzero == 0) {
			_order = EMPTY;
			return _order;
		}
		/*
		 * Copy only the portion of the array that contains indices
		 * corresponding to transactions that participate in at least one edge.
		 */
		_order = new int[nnzero];
		System.arraycopy(tmp, 0, _order, 0, nnzero);
		/*
		 * Note: sorting order[] aids debugging by ordering the behavior of
		 * updateClosure(), but it is not required for correctness.
		 */
		if (sortOrder) {
			Arrays.sort(_order);
		}
		return _order;
    }
    
    /**
	 * This is a special case version of {@link #getOrder()} that is invoked by
	 * {@link #updateClosure(int t, int u, boolean insert)} when
	 * <code>insert == true</code> and forces <code>t</code> and
	 * <code>u</code> to be included in the returned order[] even if those
	 * vertices do not participate in any edges. In order to correctly update
	 * the closure under insert, the order[] MUST contain <code>t</code> (<code>u</code>)
	 * even if that vertex does not currently participate in any edge since it
	 * will participate after the edge has been added and therefore MUST
	 * participdate in the matrix operations that update the closure of W.
	 * <p>
	 * When <code>t</code> and <code>u</code> both already participate in at
	 * least one edge, then this method simply delegates to {@link #getOrder()}.
	 * 
	 * @param t
	 *            A transaction index for which an edge is being added
	 *            <code>t WAITS_FOR u</code>.
	 * @param u
	 *            Another transaction index.
	 * @return
	 * 
	 * @see #updateClosure(int, int, boolean)
	 * @see #getOrder()
	 */
    
    final synchronized int[] getOrder( final int t, final int u )
    {
    	
    	if (t == u) {
			throw new IllegalArgumentException();
    	}

		if ((inbound[t] > 0 || outbound[t] > 0)
				&& (inbound[u] > 0 || outbound[u] > 0)) {

			/*
			 * Since both t and u are already participating in at least one edge
			 * we can simply delegate this to {@link #getOrder()}.
			 */
			
			return getOrder();
			
		}

		/*
		 * Compute #of transactions actually used in at least one edge. We do
		 * this by scanning the mapping and then verifying that each index in
		 * turn serves as either the source or the target for at least one edge.
		 * 
		 * Note: If the transaction index is either t or u then we force it to
		 * be included.
		 * 
		 * Note: We DO NOT update the _order field since that is only used to
		 * cache based on the defacto state, not when we are actively inserting
		 * an edge.
		 */

		final int n = size();
		final int[] tmp = new int[n];
		int nnzero = 0;
		final Iterator itr = mapping.values().iterator();
		while (itr.hasNext()) {
			final int index = ((Integer) itr.next()).intValue();
			if( index==t || index==u || inbound[index]>0 || outbound[index]>0 ) {
				tmp[nnzero++] = index;
			}
		}
		if (nnzero == 0) {
			return EMPTY;
		}
		/*
		 * Copy only the portion of the array that contains indices
		 * corresponding to transactions that participate in at least one edge.
		 */
		int[] order = new int[nnzero];
		System.arraycopy(tmp, 0, order, 0, nnzero);
		/*
		 * Note: sorting order[] aids debugging by ordering the behavior of
		 * updateClosure(), but it is not required for correctness.
		 */
		if (sortOrder) {
			Arrays.sort(order);
		}
		return order;
    }

    /**
	 * Resets the {@link #_order} cache so that {@link #getOrder()} will be
	 * forced to recompute its response. This method is automatically.
	 */
    final synchronized void resetOrder() {
    	_order = null;
    }
    
    /**
	 * Add a set of edges to the DAG. Each edge has the semantics
	 * <code> blocked -&gt; running[ i ]</code>, i.e., the <i>blocked </i>
	 * transaction <em>WAITS_FOR</em> the <i>running </i> transaction.
	 * 
	 * @param blocked
	 *            A transaction that is blocked waiting on one or more
	 *            transactions.
	 * 
	 * @param running
	 *            One or more transactions in the granted group for some
	 *            resource.
	 * 
	 * @exception IllegalArgumentException
	 *                If either argument is <code>null</code>.
	 * @exception IllegalArgumentException
	 *                If any element of <i>running </i> is <code>null</code>.
	 * @exception IllegalArgumentException
	 *                If <code>blocked == running[ i]</code> for any element
	 *                of <i>running </i>.
	 * @exception IllegalArgumentException
	 *                If <code>running.length</code> is greater than the
	 *                capacity of the DAG.
	 * @exception IllegalStateException
	 *                If creating the described edges would cause a duplicate
	 *                edge to be asserted (either the edge already exists or it
	 *                is defined more than once by the parameters).
	 * @exception DeadlockException
	 *                If adding the described edges to the DAG would result in a
	 *                cycle. The state of the DAG is unchanged if this exception
	 *                is thrown.
	 */

    synchronized public void addEdges( final Object blocked, final Object[] running )
		throws DeadlockException
    {
        if( running == blocked ) {
            throw new IllegalArgumentException("transaction may not wait for self");
        }
        if( running == null ) {
            throw new IllegalArgumentException("running is null");
        }
        if( running.length == 0 ) {
        	return; // NOP.
        }
        // src of the edges.
        final int src = lookup( blocked, true );
        // dst of the edges.
        final int[] dst= new int[ running.length ];
        for( int i=0; i<running.length; i++ ) {
            dst[ i ] = lookup( running[ i ], true );
            if( dst[ i ] == src ) {
            	throw new IllegalArgumentException("transaction may not wait for self");
            }
            if( W[src][dst[i]] ) {
                throw new IllegalStateException("edge exists");
            }
        }
        /*
		 * Make a backup of the in-use cells of M.  Apply changes directly to M.
		 * If a deadlock results, then restore M from the back.  (This approach
		 * presumes that deadlocks are less likely than success.)
		 */
        if( DEBUG ) {
        	log.debug(toString());
        }
        final int[] order = getOrder();
        backup( order );
        try {
			for (int i = 0; i < dst.length; i++) {
				if( ! updateClosure( src, dst[i], true ) ) {
                    log.warn("deadlock");
					if( DEBUG ) {
						log.debug(toString());
					}
//                    System.err.println(toString());
					restore(order);
					throw new DeadlockException("deadlock");
				}
			}
		} catch (DeadlockException ex) {
			/*
			 * We have already rolled back the tentative change and just rethrow
			 * this exception.
			 */
			throw ex;
		} catch (Throwable t) {
			/*
			 * Make sure that any other exception causes the tentative change to
			 * be rolled back.
			 */
			log.error(t);
			restore(order);
			throw new RuntimeException( t );
		}
		/*
		 * Post-processing to update the edge matrix W and the inbound and
		 * outbound edge count vectors.  If any of the counters were zero,
		 * then we reset the order[] cache.
		 */
		boolean reset = false;
		for( int i=0; i<dst.length; i++ ) {
			final int tgt = dst[i];
			W[src][tgt] = true; // add edge to W.
			outbound[src]++; // increment outbound counter.
			inbound[tgt]++; // increment inbound counter.
			if( outbound[src] == 1 || inbound[tgt] == 1 ) {
				/*
				 * If either counter was zero (and is now one) then the order
				 * must be reset.
				 */
				reset = true;
			}
		}
		if( reset ) {
			resetOrder();
		}
        if( DEBUG ) {
        	log.debug(toString());
        }
    }
    
    /**
	 * Return <code>true</code> iff the described edge exists in the graph.
	 * 
	 * @param blocked
	 *            A transaction.
	 * @param running
	 *            A different transaction.
	 * 
	 * @exception IllegalArgumentException
	 *                If either argument is <code>null</code>.
	 * @exception IllegalArgumentException
	 *                If the same transaction is specified for both arguments.
	 * 
	 * @return true iff the edge exists.
	 */
    synchronized public boolean hasEdge( final Object blocked, final Object running )
    {
        if( running == blocked ) {
            throw new IllegalArgumentException("transaction may not wait for itself");
        }
        int dst = lookup( running, true );
        int src = lookup( blocked, true );
        if( src == dst ) {
            throw new IllegalArgumentException("transaction may not wait for itself");
        }
        return W[src][dst];
    }
    
//    /**
//	 * Return an array of the application provided transaction objects. The
//	 * index positions in this array correspond to the transaction indices as
//	 * assigned by {@link #lookup(Object, boolean)}. The length of the array
//	 * corresponds to the #of in-use transactions.
//	 * 
//	 * @return An array of the in-use transaction objects that is indexed by the
//	 *         assigned transaction indices.
//	 */
//    
//    synchronized Object[] getTransactions() {
//        // extract index -> transaction object mapping.
//    	final int n = size();
//        final Object[] transactions = new Object[ n ];
//        final Iterator itr = mapping.entrySet().iterator();
//        while( itr.hasNext() ) {
//        	final Map.Entry entry = (Map.Entry) itr.next();
//            final int index = ((Integer)entry.getValue()).intValue();
//            transactions[index] = entry.getKey();
//        }
//        return transactions;
//    }
    
    /**
	 * Return an array of the edges asserted for the graph. The length of the
	 * array is the #of in use transactions in the graph. Each element of the
	 * array represents a single edge. The state of the returned object is
	 * current as of the time that this method executes and is not maintained.
	 * 
	 * @return Return the edges of the graph. The edges are not in any
	 *         particular order.
	 *         
	 * @see Edge
	 */
    
    synchronized public Edge[] getEdges( final boolean closure )
    {
        final int[] order = getOrder();
        final int n = order.length;
//        // extract index -> transaction object mapping.
//        Object[] transactions = getTransactions();
        // populate array of explict edges w/ optional closure.
        Vector<Edge> v = new Vector<Edge>();
        for(int i=0; i<n; i++ ) {
            for( int j=0; j<n; j++ ) {
                if( W[order[i]][order[j]] ) {
                        v.add( new Edge( transactions[order[i]], transactions[order[j]], true ) );
                } else {
                    if( closure && M[order[i]][order[j]] > 0 ) {
                        v.add( new Edge( transactions[order[i]], transactions[order[j]], false ) );
                    }
                }
            }
        }
        return (Edge[]) v.toArray( new Edge[]{} );
    }

    /**
     * A representation of an edge in the DAG used for export of information to
     * the caller.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson
     *         </a>
     */
    public static class Edge
    {
        /**
         * The transaction object for the source vertex (src WAITS_FOR tgt).
         */
        final public Object src;
        /**
         * The transaction object for the target vertex.
         */
        final Object tgt;
        /**
         * True iff the edge was explicitly asserted and false if the edge
         * was inferred by the closure of the WAITS_FOR relationship.
         */
        final boolean explicit;

        /**
         * @param src
         * @param tgt
         * @param explicit
         */
        Edge( Object src, Object tgt, boolean explicit ) {
            if( src == null || tgt == null || src == tgt ) {
                throw new IllegalArgumentException();
            }
            this.src = src;
            this.tgt = tgt;
            this.explicit = explicit;
        }
        
        /**
         * Human readable representation of the edge.
         */
        public String toString()
        {
            return ""+src+" -> "+tgt+(explicit?"":" (inferred)");
        }
        
        /**
         * The transaction object which is the source of the WAITS_FOR edge.
         */

        public Object getSource() {
        	return src;
        }
        
        /**
         * The transaction object which is the target of the WAITS_FOR edge.
         */

        public Object getTarget() {
        	return tgt;
        }
    
        /**
         * Return true iff the edge was explicitly asserted (versus implied
         * by the transitive closure of the WAITS_FOR graph).
         */
        public boolean isExplicit() {
        	return explicit;
        }
        
    }
    
    /**
	 * Removes an edge from the DAG.
	 * <p>
	 * Note: This method does NOT does not recycle the vertex associated with a
	 * transaction which no longer has any incoming or outgoing edges. See
	 * {@link #removeEdges(Object, boolean)}.
	 * 
	 * @param blocked
	 *            A transaction which is currently waiting on <i>running </i>.
	 * @param running
	 *            Another transaction.
	 * 
	 * @exception IllegalArgumentException
	 *                If either argument is <code>null</code>.
	 * @exception IllegalArgumentException
	 *                If the same transaction is specified for both arguments.
	 * @exception IllegalStateException
	 *                If the described edge does not exist.
	 */

    synchronized public void removeEdge( final Object blocked, final Object running )
    {
        final int src, tgt;
        if( ( src = lookup( blocked, false ) ) == -1 ) {
            throw new IllegalStateException("unknown transaction: tx1="+blocked);
        }
        if( ( tgt = lookup( running, false ) ) == -1 ) {
            throw new IllegalStateException("unknown transaction: tx2="+running);
        }
        if( src == tgt ) {
            throw new IllegalArgumentException();
        }
        if( ! W[src][tgt] ) {
            throw new IllegalStateException("edge does not exist: src="+blocked+", tgt="+running);
        }
    	/*
		 * Note: This method does not preserve the internal state of the DAG
		 * against an exception thrown by updateClosure. The reason for this is
		 * that updateClosure should not be able to indicate a deadlock or cause
		 * an ArithmeticException when removing edges.  In order to be able to
		 * protect against wild hairs we would have to backup M before removing
		 * an edge, and that causes too much overhead for something which "should
		 * not" happen.
		 */
        if( DEBUG ) {
        	log.debug(toString());
        }
        // update the closure.
        updateClosure( src, tgt, false );
        // remove the edge.
        W[src][tgt] = false;
        outbound[src]--;
        inbound[tgt]--;
        if(outbound[src] == 0 || inbound[tgt] == 0 ) {
        	resetOrder();
        }
        if(outbound[src] <0 ) {
        	throw new AssertionError();
        }
        if( inbound[tgt] <0 ) {
        	throw new AssertionError();
        }
        if( DEBUG ) {
        	log.debug(toString());
        }
    }

    /**
	 * Package private method removes an edge and updates the path count matrix
	 * and the inbound and outbound edge counts for the source and target
	 * vertices. The described edge must exist.
	 * 
	 * @param src
	 *            The source vertex.
	 * @param tgt
	 *            The target vertex.
	 */
    
    private synchronized void removeEdge( final int src, final int tgt )
    {
        if( src == tgt ) {
            throw new IllegalArgumentException();
        }
        if( ! W[src][tgt] ) {
            throw new IllegalStateException("edge does not exist: src="+src+", tgt="+tgt);
        }
        // update the closure.
        updateClosure( src, tgt, false );
        // remove the edge.
        W[src][tgt] = false;
        outbound[src]--;
        inbound[tgt]--;
        if(outbound[src] == 0 || inbound[tgt] == 0 ) {
        	resetOrder();
        }
        if(outbound[src] <0 ) {
        	throw new AssertionError();
        }
        if( inbound[tgt] <0 ) {
        	throw new AssertionError();
        }
    }

    /**
     * Remove all edges whose target is <i>tx</i>. This method SHOULD be used
     * when a running transaction completes (either aborts or commits). After
     * calling this method, the transaction is removed completely from the DAG.
     * Failure to use this method will result in the capacity of the DAG being
     * consumed as vertices will not be recycled unless you call
     * {@link #releaseVertex(Object)}.
     * 
     * @param tx
     *            A transaction.
     * 
     * @param waiting
     *            When false, caller asserts that this transaction it is NOT
     *            waiting on any other transaction. This assertion is used to
     *            optimize the update of the path count matrix by simply
     *            removing the row and column associated with this transaction.
     *            When [waiting == true], a less efficient procedure is used to
     *            update the path count matrix.
     *            <p>
     *            Do NOT specify [waiting == false] unless you <em>know</em>
     *            that the transaction is NOT waiting. In general, this
     *            knowledge is available to the 2PL locking package.
     * 
     * @todo Write test cases for this method. It duplicates much of the logic
     *       of {@link #removeEdge(Object, Object)} and therefore must be
     *       evaluated separately.
     */
    synchronized public void removeEdges( final Object tx, final boolean waiting )
    {
        final int tgt;
        if( ( tgt = lookup( tx, false ) ) == -1 ) {
        	// No edges for that tx.
        	return;
//            throw new IllegalStateException("unknown transaction: tx1="+tx);
        }
        if( DEBUG ) {
        	log.debug(toString());
        }
    	if( ! waiting ) {
    		/*
			 * Clear the row and column for this transaction. This only visits
			 * those transactions that have declared inbound or outbound edges.
			 * We update the inbound and outbound edge counters for the vertices
			 * that had edges involving [tgt] and clear the inbound and outbound
			 * edge counters for the [tgt] vertex.
			 */
    		final int[] order = getOrder();
    		for( int i=0; i<order.length; i++ ) {
    			final int oi = order[i];
    			if(W[tgt][oi]) {
    				inbound[oi]--;
    				if(inbound[oi]<0) {
    					throw new AssertionError();
    				}
    			}
    			if(W[oi][tgt]) {
    				outbound[oi]--;
    				if(outbound[oi]<0) {
    					throw new AssertionError();
    				}
    			}
    			M[oi][tgt] = M[tgt][oi] = 0; // zero path counts.
    			W[oi][tgt] = W[tgt][oi] = false; // remove edge.
    		}
    		// clear counters for the vertex that whose edges were cleared.
			inbound[tgt] = 0;
			outbound[tgt] = 0;
			// reset order since the vertex is no longer part of any edge.
			resetOrder();
    	} else {
    		/*
			 * Scan the row and column in the WAITS_FOR graph for this vertex
			 * and remove each edge found therein one by one. This only visits
			 * those transactions that have declared inbound or outbound edges.
			 */
    		final int[] order = getOrder();
    		for( int i=0; i<order.length; i++ ) {
    			final int oi = order[i];
    			if( W[oi][tgt] ) {
    				// oi WAITS_FOR tgt
    		        removeEdge( oi, tgt );
    			}
    			if( W[tgt][oi] ) {
    				// tgt WAITS_FOR oi
    		        removeEdge( tgt, oi );
    			}
    		}
    	}
    	/* Remove the vertex from the mapping since all edges for that vertex
    	 * have been removed.
    	 */
    	if(!releaseVertex( tx )) {
    	    throw new AssertionError("Unknown vertex="+tx);   
        }
        if( DEBUG ) {
        	log.debug(toString());
        }
    }

}
