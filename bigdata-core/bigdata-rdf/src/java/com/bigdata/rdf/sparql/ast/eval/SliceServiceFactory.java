/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Sep 9, 2011
 */

package com.bigdata.rdf.sparql.ast.eval;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.bindingSet.EmptyBindingSet;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.btree.BTree;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.cache.ConcurrentWeakValueCacheWithTimeout;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.RangeBOp;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.service.BigdataNativeServiceOptions;
import com.bigdata.rdf.sparql.ast.service.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.spo.DistinctMultiTermAdvancer;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.relation.accesspath.EmptyCloseableIterator;
import com.bigdata.relation.accesspath.ThickCloseableIterator;
import com.bigdata.util.BytesUtil;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A factory for a statement pattern slicing service. 
 * It accepts a group with a single triple pattern in it:
 * <pre>
 * service bd:slice {
 *   ?s rdf:type ex:Foo .
 *   
 *   # required service params for the sample
 *     # either offset+limit 
 *     bd:serviceParam bd:slice.offset 0 .
 *     bd:serviceParam bd:slice.limit 2000 .
 *     # or range
 *     bd:serviceParam bd:slice.range ?range 
 * }
 * </pre>
 * The service params are required and set the slicing parameters.  You can
 * either request a slice or request a range count depending on the params.
 * The range count is useful when dealing with a "rangeSafe" predicate with
 * a range filter.
 * 
 * @see RangeBOp
 */
public class SliceServiceFactory extends AbstractServiceFactory {

    private static final Logger log = Logger
            .getLogger(SliceServiceFactory.class);

    /**
     * The URI service key.
     */
    public static final URI SERVICE_KEY = new URIImpl(BD.NAMESPACE+"slice");
    
    /**
     * The service params for this service.
     */
    public static interface SliceParams {
    	
    	/**
    	 * The offset into the range.
    	 */
    	URI OFFSET = new URIImpl(SERVICE_KEY.stringValue() + ".offset");
    	
    	/**
    	 * Default = 0.
    	 */
    	long DEFAULT_OFFSET = 0;
    	
    	/**
    	 * The limit on the slice.
    	 */
    	URI LIMIT = new URIImpl(SERVICE_KEY.stringValue() + ".limit");
    	
    	/**
    	 * Default = 1000.
    	 */
    	int DEFAULT_LIMIT = 1000;
    	
    	/**
    	 * A range request - object will be the variable to bind to the range
    	 * count.
    	 */
    	URI RANGE = new URIImpl(SERVICE_KEY.stringValue() + ".range");
    	
    }
    
    /**
     * Keep a timeout cache of start and end indices for a give predicate.
     * Typically these slice calls happen multiple times in a row in a very
     * short time period, so it's best to not have to go back to the index
     * every time for this information.
     */
    private static final ConcurrentWeakValueCacheWithTimeout<IPredicate<ISPO>, CacheHit> cache;
    
    private static final class CacheHit {
    	
    	final long startIndex, endIndex;
    	
    	public CacheHit(final long startIndex, final long endIndex) {
    		this.startIndex = startIndex;
    		this.endIndex = endIndex;
    	}
    	
    }
    
    static {

    	cache = new ConcurrentWeakValueCacheWithTimeout<IPredicate<ISPO>, CacheHit>(
    			100, TimeUnit.MINUTES.toMillis(1));
    	
    }
    
    /*
     * Note: This could extend the base class to allow for search service
     * configuration options.
     */
    private final BigdataNativeServiceOptions serviceOptions;

    public SliceServiceFactory() {
        
        serviceOptions = new BigdataNativeServiceOptions();
//        serviceOptions.setRunFirst(true);
        
    }
    
    @Override
    public BigdataNativeServiceOptions getServiceOptions() {

        return serviceOptions;
        
    }

    @Override
    public BigdataServiceCall create(final ServiceCallCreateParams params,
    		final ServiceParams serviceParams) {
        
        final AbstractTripleStore store = params.getTripleStore();

        final ServiceNode serviceNode = params.getServiceNode();
        
        /*
         * Validate the predicates for a given service call.
         */
        final StatementPatternNode sp = verifyGraphPattern(
                store, serviceNode.getGraphPattern(), serviceParams);

        /*
         * Create and return the ServiceCall object which will execute this
         * query.
         */
        return new SliceCall(store, sp, serviceOptions, serviceParams);
        
    }
    
    /**
     * Verify that there is only a single statement pattern node and that the
     * service parameters are valid.
     */
    private StatementPatternNode verifyGraphPattern(
            final AbstractTripleStore database,
            final GroupNodeBase<IGroupMemberNode> group,
            final ServiceParams params) {

    	final Iterator<Map.Entry<URI, List<TermNode>>> it = params.iterator();
    	
    	while (it.hasNext()) {
    	
    		final URI param = it.next().getKey();
    		
    		if (SliceParams.OFFSET.equals(param)) {
    			
    			if (params.getAsLong(param, null) == null) {
    				throw new RuntimeException("must provide a value for: " + param);
    			}
    			
    		} else if (SliceParams.LIMIT.equals(param)) {
    			
    			if (params.getAsInt(param, null) == null) {
    				throw new RuntimeException("must provide a value for: " + param);
    			}
    			
    		} else if (SliceParams.RANGE.equals(param)) {
    			
    			if (params.getAsVar(param, null) == null) {
    				throw new RuntimeException("must provide a variable for: " + param);
    			}
    			
    		} else {
    			
    			throw new RuntimeException("unrecognized param: " + param);
    			
    		}
    		
    	}
    	
    	StatementPatternNode sp = null;
    	
    	for (IGroupMemberNode node : group) {
    		
    		if (node instanceof FilterNode) {
    			
    			// ok to have filters with ranges
    			continue;
    			
    		}
    		
    		if (!(node instanceof StatementPatternNode)) {
    			
    			throw new RuntimeException("only statement patterns allowed");
    			
    		}
    		
    		final StatementPatternNode tmp = (StatementPatternNode) node;
    		
    		if (tmp.s().isConstant() && BD.SERVICE_PARAM.equals(tmp.s().getValue())) {
    			
    			continue;
    			
    		}
    		
    		if (sp != null) {
    			
            	throw new RuntimeException("group must contain a single statement pattern");
    			
    		}
    		
    		sp = tmp;
    		
    	}
    	
        return sp;

    }

    /**
     * 
     * Note: This has the {@link AbstractTripleStore} reference attached. This
     * is not a {@link Serializable} object. It MUST run on the query
     * controller.
     */
    private static class SliceCall implements BigdataServiceCall {

        private final AbstractTripleStore db;
        private final StatementPatternNode sp;
        private final IServiceOptions serviceOptions;
        private final ServiceParams serviceParams;
        
        public SliceCall(
                final AbstractTripleStore db,
                final StatementPatternNode sp,
                final IServiceOptions serviceOptions,
                final ServiceParams serviceParams) {

            if(db == null)
                throw new IllegalArgumentException();

            if(sp == null)
                throw new IllegalArgumentException();

            if(serviceOptions == null)
                throw new IllegalArgumentException();
            
            if(serviceParams == null)
                throw new IllegalArgumentException();

            this.db = db;
            this.sp = sp;
            this.serviceOptions = serviceOptions;
            this.serviceParams = serviceParams;
            
        }

        /**
         * Run a slice over an access path.  Currently only implemented to
         * work with zero or one incoming bindings, and all variables in the
         * incoming binding must be in use in the statement pattern.
         */
        @Override
        public ICloseableIterator<IBindingSet> call(
                final IBindingSet[] bc) {

        	if (log.isInfoEnabled()) {
        		log.info(bc.length);
        		log.info(Arrays.toString(bc));
        	}
        	
        	if (bc != null && bc.length > 1) {
        		throw new RuntimeException("cannot run with multiple incoming bindings");
        	}
        	
        	/*
        	 * Keep a map of variables in the statement pattern to the position
        	 * in which they appear in the statement pattern.
        	 */
        	final Map<IVariable, Integer> vars = new LinkedHashMap<IVariable, Integer>();
        	
        	for (int i = 0; i < sp.arity(); i++) {
        		
        		final TermNode term = sp.get(i);
        		
        		if (term == null)
        			continue;
        		
        		if (term.isVariable()) {
        			
        			final IVariable v = (IVariable) term.getValueExpression();
        			
        			if (log.isTraceEnabled()) {
        				log.trace("variable: " + v + " at position: " + i);
        			}
        			
        			vars.put(v, i);
        			
        		}
        		
        	}
        	
        	final IBindingSet bs;
        	if (bc.length == 1 && !bc[0].equals(EmptyBindingSet.INSTANCE)) {
        		
        		bs = bc[0];
        		
        	} else {
        		
        		bs = null;
        		
        	}
        	
        	if (bs != null) {
        		
        		@SuppressWarnings("rawtypes")
				final Iterator<IVariable> it = bs.vars();
        		
        		while (it.hasNext()) {
        			
        			@SuppressWarnings("rawtypes")
					final IVariable v = it.next();
        			
        			if (!vars.containsKey(v)) {
        				
        				throw new RuntimeException("unrecognized variable in incoming binding");
        				
        			}
        			
        			if (bs.isBound(v)) {
        				
            			// no longer a variable
        				vars.remove(v);
        				
        			}
        			
        		}
        		
        	}

        	// Handle a range.
        	final RangeBOp rangeBOp = sp.getRange() != null ? sp.getRange().getRangeBOp() : null;
        	
        	if (log.isTraceEnabled()) {
        		log.trace("range: " + rangeBOp);
        	}
        	
        	// Create the predicate.
    		@SuppressWarnings("unchecked")
			final IPredicate<ISPO> pred = (IPredicate<ISPO>)
    				db.getSPORelation().getPredicate(
    						getTerm(sp, bs, 0),
    						getTerm(sp, bs, 1),
    						getTerm(sp, bs, 2),
    						getTerm(sp, bs, 3),
    						null,
    						rangeBOp
    						);
    		
    		if (pred == null) {
    			
    			return new EmptyCloseableIterator<IBindingSet>();
    			
    		}
    		
    		// Get the right key order for the predicate.
    		final SPOKeyOrder keyOrder = db.getSPORelation().getKeyOrder(pred);

    		// Grab the corresponding index.
    		final BTree ndx = (BTree) db.getSPORelation().getIndex(keyOrder);
    		
    		/*
    		 * Inspect the cache and/or the index for the starting and ending 
    		 * tuple index for this access path.
    		 */
            final long startIndex, endIndex;
            
            /*
             * Avoid an index read if possible.
             */
    		final CacheHit hit = cache.get(pred);
    		
    		if (hit == null) {
    		
    			if (log.isTraceEnabled()) {
    				log.trace("going to index for range");
    			}
    			
	    		final byte[] startKey = keyOrder.getFromKey(KeyBuilder.newInstance(), pred);

	    		startIndex = indexOf(ndx, startKey);
	
	    		final byte[] endKey = keyOrder.getToKey(KeyBuilder.newInstance(), pred); //SuccessorUtil.successor(startKey.clone());
	    		
	    		endIndex = indexOf(ndx, endKey) - 1;
	    		
	            cache.put(pred, new CacheHit(startIndex, endIndex));
	            
    		} else {
    			
    			if (log.isTraceEnabled()) {
    				log.trace("cache hit");
    			}
    			
    			startIndex = hit.startIndex;
    			
    			endIndex = hit.endIndex;
    			
    		}
            
            final long range = endIndex - startIndex + 1;
            
    		if (log.isTraceEnabled()) {
    			log.trace("range: " + range);
    		}
    		
    		/*
    		 * Caller is asking for a range count only.
    		 */
    		if (serviceParams.contains(SliceParams.RANGE)) {
    			
    			final IVariable<IV> v = serviceParams.getAsVar(SliceParams.RANGE);
    			
    			final IBindingSet[] bSets = new IBindingSet[1];
    			
    			bSets[0] = bs != null ? bs.clone() : new ListBindingSet();
    			
    			bSets[0].set(v, new Constant<IV>(new XSDNumericIV(range)));
    			
    			return new ThickCloseableIterator<IBindingSet>(bSets, 1); 
    			
    		}
    		
    		final long offset = serviceParams.getAsLong(
    				SliceParams.OFFSET, SliceParams.DEFAULT_OFFSET);
    		
    		if (offset < 0) {
    			
    			throw new RuntimeException("illegal negative offset");
    			
    		}
    		
    		if (offset > range) {
    			
    			throw new RuntimeException("offset is out of range");
    			
    		}
    		
    		final int limit = serviceParams.getAsInt(
    				SliceParams.LIMIT, SliceParams.DEFAULT_LIMIT);
    		
    		if (log.isTraceEnabled()) {
    			log.trace("offset: " + offset);
    			log.trace("limit: " + limit);
    		}

    		/*
    		 * Reading from the startIndex plus the offset.
    		 */
    		final long fromIndex = Math.max(startIndex, startIndex + offset);
    		
    		/*
    		 * Reading to the offset plus the limit (minus 1), or the end
    		 * index, whichever is smaller.
    		 */
    		final long toIndex = Math.min(startIndex + offset + limit - 1,
    									   endIndex);
    		
    		if (fromIndex > toIndex) {
    			
    			throw new RuntimeException("fromIndex > toIndex");
    			
    		}
    		
            final byte[] fromKey = ndx.keyAt(fromIndex);
            
            final byte[] toKey = SuccessorUtil.successor(ndx.keyAt(toIndex));

            final int arity = pred.arity();
            
            final int numBoundEntries = pred.arity() - vars.size();
            
    		if (log.isTraceEnabled()) {
    			log.trace("fromIndex: " + fromIndex);
    			log.trace("toIndex: " + toIndex);
                log.trace("fromKey: " + BytesUtil.toString(fromKey));
                log.trace("toKey: " + BytesUtil.toString(toKey));
                log.trace("arity: " + arity);
                log.trace("#boundEntries: " + numBoundEntries);
                log.trace(keyOrder);
    		}

    		/*
    		 * Use a multi-term advancer to skip the bound entries and just
    		 * get to the variables.
    		 * 
    		 * Not a good idea.  Needs to visit each tuple individually.
    		 */
            final DistinctMultiTermAdvancer advancer = null;
//                    new DistinctMultiTermAdvancer(
//                            arity, //arity - 3 or 4
//                            numBoundEntries // #boundEntries - anything not a var and not bound by incoming bindings
//                            );
//            final DistinctTermAdvancer advancer2 = 
//            		  new DistinctTermAdvancer(arity);

            final ITupleIterator it = ndx.rangeIterator(fromKey, toKey,
                    0/* capacity */, IRangeQuery.KEYS | IRangeQuery.CURSOR, advancer);
            
            /*
             * Max # of tuples read will be limit.
             */
            final IBindingSet[] bSets = new IBindingSet[limit];
            
            int i = 0;
            
            while (it.hasNext()) {
            	
            	final byte[] key = it.next().getKey();
            	
            	final SPO spo = keyOrder.decodeKey(key);
            	
        		bSets[i] = bs != null ? bs.clone() : new ListBindingSet();
                		
        		for (IVariable v : vars.keySet()) {
        			
        			final int pos = vars.get(v);
        			
        			bSets[i].set(v, new Constant<IV>(spo.get(pos)));
        			
        		}
        		
//            	if (log.isTraceEnabled()) {
//            		log.trace("next bs: " + bSets[i]);
//            	}
            	
            	i++;
            	
            }

        	if (log.isTraceEnabled()) {
        		log.trace("done iterating " + i + " results.");
        	}
        	
            return new ThickCloseableIterator<IBindingSet>(bSets, i); 

        }
        
        /**
         * Get the IV in the statement pattern at the specified position, or
         * get the value from the binding set for the variable at that position.
         * Return null if not bound in either place. 
         */
        private IV getTerm(final StatementPatternNode sp, final IBindingSet bs, final int pos) {
        	
        	final TermNode t = sp.get(pos);
        	
        	if (t == null)
        		return null;
        	
        	if (t.isConstant()) {
        		
        		return ((IConstant<IV>) t.getValueExpression()).get();
        		
        	} else {
        		
        		final IVariable<IV> v = (IVariable<IV>) t.getValueExpression();
        		
        		if (bs != null && bs.isBound(v)) {
        			
        			return ((IConstant<IV>) bs.get(v)).get();
        			
        		} else {
        			
        			return null;
        			
        		}
        		
        	}
        	
        }
         
        /**
         * Use the index to find the index of the tuple for the specified key
         * (or the index of the next real tuple after the specified key).
         */
        private long indexOf(final BTree ndx, final byte[] key) {
        	
            if (log.isTraceEnabled()) {
                log.trace(BytesUtil.toString(key));
            }
            
            final long indexOfKey = ndx.indexOf(key);
            
            if (log.isTraceEnabled()) {
                log.trace("result of indexOf(key): " + indexOfKey);
            }
            
            final long index;
            if (indexOfKey >= 0) {
                // it's a real key
                index = indexOfKey;
            } else {
                // not a real key
                index = -(indexOfKey+1);
            }

            if (log.isTraceEnabled()) {
                log.trace("index: " + index);
            }
            
            return index;

        }
        
        @Override
        public IServiceOptions getServiceOptions() {
            
            return serviceOptions;
            
        }
        
    }

}
