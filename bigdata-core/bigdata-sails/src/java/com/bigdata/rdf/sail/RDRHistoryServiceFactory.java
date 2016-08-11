package com.bigdata.rdf.sail;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;
import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.impl.bnode.SidIV;
import com.bigdata.rdf.sail.BigdataSail.BigdataSailConnection;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.service.BigdataNativeServiceOptions;
import com.bigdata.rdf.sparql.ast.service.BigdataServiceCall;
import com.bigdata.rdf.sparql.ast.service.CustomServiceFactory;
import com.bigdata.rdf.sparql.ast.service.IServiceOptions;
import com.bigdata.rdf.sparql.ast.service.ServiceCall;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.striterator.ChunkedWrappedIterator;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.IStriterator;
import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * 
 */
public class RDRHistoryServiceFactory implements CustomServiceFactory {

    static private transient final Logger log = Logger
            .getLogger(RDRHistoryServiceFactory.class);

    private final BigdataNativeServiceOptions serviceOptions;

    public RDRHistoryServiceFactory() {

        serviceOptions = new BigdataNativeServiceOptions();
        
        /*
         * TODO Review decision to make this a runFirst service. The rational is
         * that this service can only apply a very limited set of restrictions
         * during query, therefore it will often make sense to run it first.
         * However, the fromTime and toTime could be bound by the query and the
         * service can filter some things more efficiently internally than if we
         * generated a bunch of intermediate solutions for those things.
         */
        serviceOptions.setRunFirst(true);
        
    }

    @Override
    public IServiceOptions getServiceOptions() {

        return serviceOptions;

    }

    /**
     * Instantiate the service call with the supplied params.
     */
    @Override
    public ServiceCall<?> create(final ServiceCallCreateParams params) {

//        if (params == null)
//            throw new IllegalArgumentException();
//
//        final AbstractTripleStore store = params.getTripleStore();
//
//        if (store == null)
//            throw new IllegalArgumentException();
//
//        final ServiceNode serviceNode = params.getServiceNode();
//
//        if (serviceNode == null)
//            throw new IllegalArgumentException();
//
//        final GraphPatternGroup<IGroupMemberNode> group =
//                serviceNode.getGraphPattern();
//        
//        verifyGroup(group);
//        
//        return new RDRHistoryServiceCall(store, getServiceOptions(), group);

        throw new UnsupportedOperationException("deprecated");
        
    }
    
    /**
     * Make sure we've got the right stuff in there - two statement patterns
     * plus optional filters.
     */
    private void verifyGroup(final GraphPatternGroup<IGroupMemberNode> group) {
        
        if (log.isDebugEnabled()) {
            log.debug(group);
        }
        
    }

    /**
     * Register an {@link IChangeLog} listener that will manage the maintenance
     * of the describe cache.
     */
    @Override
    public void startConnection(final BigdataSailConnection conn) {

        final AbstractTripleStore database = conn.getTripleStore();
        if (database.isRDRHistory()) {
            final RDRHistory history = database.getRDRHistoryInstance();
            history.init();
            conn.addChangeLog(history);
        }

    }
    
    private static class RDRHistoryServiceCall implements BigdataServiceCall {

        private final AbstractTripleStore database;
        private final IServiceOptions options;
        private final GraphPatternGroup<IGroupMemberNode> subgroup;
        
        private final StatementPatternNode sidNode;
        private final StatementPatternNode historyNode;
        
        public RDRHistoryServiceCall(final AbstractTripleStore database,
                final IServiceOptions options,
                final GraphPatternGroup<IGroupMemberNode> subgroup) {
            this.database = database;
            this.options = options;
            this.subgroup = subgroup;
            
            final List<StatementPatternNode> spNodes =
                    subgroup.getChildren(StatementPatternNode.class);
            if (spNodes.size() != 2) {
                throw new IllegalArgumentException();
            }
            if (spNodes.get(0).sid() != null) {
                sidNode = spNodes.get(0);
                historyNode = spNodes.get(1);
            } else {
                sidNode = spNodes.get(1);
                historyNode = spNodes.get(0);
            }
        }
        
        @Override
        public IServiceOptions getServiceOptions() {
            return options;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public ICloseableIterator<IBindingSet> call(
                final IBindingSet[] bindingSets) throws Exception {
            
            /*
             * << <s> ?p ?o >> ?action ?time . # SPO
             * << <s> ?p ?o >> <added> ?time . # SPO + filter
             * << <s> ?p ?o >> <added> "t" . # SPO + filter
             */
            final Predicate<?> pred = new Predicate(new BOp[] { 
                    sidNode.s().getValueExpression(),
                    sidNode.p().getValueExpression(),
                    sidNode.o().getValueExpression(),
                    historyNode.p().getValueExpression(),
                    historyNode.o().getValueExpression()
            });
            final byte flags = SidIV.toFlags();
            
            final IStriterator results = new Striterator(Collections.emptyIterator());
            
            final Map<Predicate, List<IBindingSet>> coalesced =
                    new LinkedHashMap<Predicate, List<IBindingSet>>();
            
            for (final IBindingSet bs : bindingSets) {
                
                final Predicate<?> asBound = pred.asBound(bs);
                
                final List<IBindingSet> values;
                if (coalesced.containsKey(asBound)) {
                    values = coalesced.get(asBound);
                } else {
                    coalesced.put(asBound, values = new LinkedList<IBindingSet>());
                }
                values.add(bs);
                
            }
            
            final IV[] ivs = new IV[5];
            
            for (final Entry<Predicate,List<IBindingSet>> entry : coalesced.entrySet()) {
             
                final Predicate asBound = entry.getKey();
                
                final SPOKeyOrder keyOrder = SPOKeyOrder.SPO;
                
                final IIndex ndx = database.getSPORelation().getIndex(keyOrder);
                
                final IKeyBuilder keyBuilder = 
                        ndx.getIndexMetadata().getKeyBuilder();
                
                keyBuilder.reset();
                
                keyBuilder.appendSigned(flags);

                for (int i = 0; i < 5; i++) {
                    final IVariableOrConstant term = asBound.get(i);
                    if (term.isConstant()) {
                        final IV iv = ((Constant<IV>) term).get();
                        IVUtility.encode(keyBuilder, iv);
                    } else {
                        break;
                    }
                }
                
                final byte[] fromKey = keyBuilder.getKey();
                final byte[] toKey = SuccessorUtil.successor(fromKey.clone()); 
                
                final ITupleIterator<ISPO> titr = ndx.rangeIterator(fromKey, toKey,
                        0/* capacity */, IRangeQuery.DEFAULT | IRangeQuery.KEYS, 
                        null/* filter */);

                final IStriterator sitr = new Striterator(titr);
                
                /*
                 * Resolve ITuple -> ISPO.
                 */
                sitr.addFilter(new Resolver() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    protected Object resolve(final Object e) {
                        final ITuple<ISPO> t = (ITuple<ISPO>) e;
                        return t.getObject();
                    }
                });
                
                /*
                 * Filter against bound terms.
                 */
                sitr.addFilter(new Filter() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public boolean isValid(final Object e) {
                        toIVs((ISPO) e, ivs);
                        /*
                         * Compare against the asBound predicate.
                         */
                        for (int i = 0; i < 5; i++) {
                            final IVariableOrConstant term = asBound.get(i);
                            if (term.isConstant()) {
                                final IV iv = ((Constant<IV>) term).get();
                                if (!iv.equals(ivs[i])) {
                                    return false;
                                }
                            }
                        }
                        return true;
                    }
                });
                
                /*
                 * Resolve ISPO -> IBindingSet (one to many).
                 */
                sitr.addFilter(new Expander() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    protected Iterator expand(final Object e) {
                        toIVs((ISPO) e, ivs);
                        final Striterator it = new Striterator(
                                entry.getValue().iterator());
                        it.addFilter(new Resolver() {
                            private static final long serialVersionUID = 1L;
                            @Override
                            protected Object resolve(final Object e) {
                                final IBindingSet bs = ((IBindingSet) e).clone();
                                /*
                                 * Bind variables in the result.
                                 */
                                for (int i = 0; i < 5; i++) {
                                    final IVariableOrConstant term = asBound.get(i);
                                    if (term.isVar()) {
                                        final IVariable var = (IVariable<IV>) term;
                                        bs.set(var, new Constant<IV>(ivs[i]));
                                    }
                                }
                                return bs;
                            }
                        });
                        return it;
                    }
                });
                
                results.append(sitr);
                
            }
            
            return new ChunkedWrappedIterator<IBindingSet>(results);
            
        }
        
        @SuppressWarnings("rawtypes")
        private void toIVs(final ISPO spo, final IV[] ivs) {
            final ISPO sid = ((SidIV) spo.s()).getInlineValue();
            ivs[0] = sid.s();
            ivs[1] = sid.p();
            ivs[2] = sid.o();
            ivs[3] = spo.p();
            ivs[4] = spo.o();
        }
        
    }

    
    //TODO:  See https://jira.blazegraph.com/browse/BLZG-1360
	@Override
	public Set<IVariable<?>> getRequiredBound(ServiceNode serviceNode) {
		
		throw new RuntimeException("Method not yet implemented.");

	}

    //TODO:  See https://jira.blazegraph.com/browse/BLZG-1360

	@Override
	public Set<IVariable<?>> getDesiredBound(ServiceNode serviceNode) {
		throw new RuntimeException("Method not yet implemented.");
	}

} 
