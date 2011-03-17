package com.bigdata.rdf.sail.sop;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.SingletonSet;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.ValueExpr;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sail.sop.SOpTree.SOpGroup;
import com.bigdata.rdf.sail.sop.SOpTree.SOpGroups;

public class SOpTreeBuilder {

	public static final int ROOT_GROUP_ID = 0;
	
	private final AtomicInteger groupId = new AtomicInteger();
	
	private final AtomicInteger sopId = new AtomicInteger();
	
	public SOpTreeBuilder() {
	}
	
	public SOpTree pruneGroups(final SOpTree tree, 
			final Collection<SOpGroup> groupsToPrune) {
		final Collection<SOpGroup> children = new LinkedList<SOpGroup>();
		for (SOpGroup g : groupsToPrune)
			collectChildren(tree, g, children);
		groupsToPrune.addAll(children);
		final Collection<SOp> sopsToPrune = new LinkedList<SOp>();
		for (SOpGroup g : groupsToPrune) {
			for (SOp sop : g) {
				sopsToPrune.add(sop);
			}
		}
		return pruneSOps(tree, sopsToPrune);
	}

	private void collectChildren(final SOpTree tree, final SOpGroup group,
			final Collection<SOpGroup> groups) {
		final SOpGroups children = tree.getChildren(group);
		if (children != null) {
			for (SOpGroup g : tree.getChildren(group)) {
				groups.add(g);
				collectChildren(tree, g, groups);
			}
		}
	}
	
	public SOpTree pruneSOps(final SOpTree tree, 
			final Collection<SOp> sopsToPrune) {
		final Collection<SOp> sops = new LinkedList<SOp>();
		for (SOp sop : tree) {
			if (!sopsToPrune.contains(sop)) {
				sops.add(sop);
			}
		}
		return new SOpTree(sops);
	}
	
    public synchronized SOpTree collectSOps(final TupleExpr root) {
    	
    	/*
    	 * Note: The ids are assigned using incrementAndGet() so ZERO (0) is
    	 * the first id that will be assigned when we pass in (-1) as
    	 * the initial state of the AtomicInteger.
    	 */
    	groupId.set(0);
    	sopId.set(999);

    	final List<SOp> sops = new LinkedList<SOp>();
        collectSOps(sops, root, false, 0, -1);
        return new SOpTree(sops);
        
    }
    
    private void collectSOps(final List<SOp> sops, final TupleExpr tupleExpr, 
    		final boolean rslj, final int g, final int pg) {
    	
    	if (tupleExpr instanceof StatementPattern) {
    		final StatementPattern sp = (StatementPattern) tupleExpr;
    		collectSOps(sops, sp, rslj, g, pg);
    	} else if (tupleExpr instanceof Filter) {
    		final Filter filter = (Filter) tupleExpr;
    		collectSOps(sops, filter, rslj, g, pg);
    	} else if (tupleExpr instanceof Join) {
    		final Join join = (Join) tupleExpr;
    		collectSOps(sops, join, rslj, g, pg);
    	} else if (tupleExpr instanceof LeftJoin) {
    		final LeftJoin leftJoin = (LeftJoin) tupleExpr;
    		collectSOps(sops, leftJoin, rslj, g, pg);
    	} else if (tupleExpr instanceof Union) {
    		final Union union = (Union) tupleExpr;
    		collectSOps(sops, union, rslj, g, pg);
    	} else {    	
    		throw new UnsupportedOperatorException(tupleExpr);
    	}
    	
    }
    
    private void collectSOps(final List<SOp> sops, final LeftJoin leftJoin, 
            final boolean rslj, final int g, final int pg) {
        
        final TupleExpr left = leftJoin.getLeftArg();
        
        if (left instanceof StatementPattern) {
            collectSOps(sops, (StatementPattern) left, rslj, g, pg);
        } else if (left instanceof Filter) {
            collectSOps(sops, (Filter) left, rslj, g, pg);
        } else if (left instanceof Join) {
            collectSOps(sops, (Join) left, rslj, g, pg);
        } else if (left instanceof LeftJoin) {
            collectSOps(sops, (LeftJoin) left, rslj, g, pg);
        } else if (left instanceof SingletonSet){
            // do nothing
        } else if (left instanceof Union){
            collectSOps(sops, (Union) left, rslj, groupId.incrementAndGet(), g);
        } else {
            throw new UnsupportedOperatorException(left);
        }
        
        final TupleExpr right = leftJoin.getRightArg();

        final int rsgId, rspgId;
        
        if (right instanceof StatementPattern) {
            if (left instanceof SingletonSet)
                collectSOps(sops, (StatementPattern) right, true, rsgId=g, rspgId=pg);
            else
            	collectSOps(sops, (StatementPattern) right, true, rsgId=groupId.incrementAndGet(), rspgId=g);
        } else if (right instanceof Filter) {
            collectSOps(sops, (Filter) right, true, rsgId=groupId.incrementAndGet(), rspgId=g);
        } else if (right instanceof Join) {
            collectSOps(sops, (Join) right, true, rsgId=groupId.incrementAndGet(), rspgId=g);
        } else if (right instanceof LeftJoin) {
            if (left instanceof SingletonSet)
                collectSOps(sops, (LeftJoin) right, true, rsgId=g, rspgId=pg);
            else
                collectSOps(sops, (LeftJoin) right, true, rsgId=groupId.incrementAndGet(), rspgId=g);
        } else if (right instanceof Union) {
            collectSOps(sops, (Union) right, true, rsgId=groupId.incrementAndGet(), rspgId=g);
        } else {
            throw new UnsupportedOperatorException(right);
        }
        
        final ValueExpr ve = leftJoin.getCondition();
        // conditional for tails in this group
        if (ve != null) {
        	sops.add(new SOp(sopId.incrementAndGet(), ve, rsgId, rspgId, true));
        }
        
    }
    
    private void collectSOps(final List<SOp> sops, final Join join, 
            final boolean rslj, final int g, final int pg) {
        
        final TupleExpr left = join.getLeftArg();
        
        if (left instanceof StatementPattern) {
            collectSOps(sops, (StatementPattern) left, rslj, g, pg);
        } else if (left instanceof Filter) {
            collectSOps(sops, (Filter) left, rslj, g, pg);
        } else if (left instanceof Join) {
            collectSOps(sops, (Join) left, rslj, g, pg);
        } else if (left instanceof LeftJoin) {
            collectSOps(sops, (LeftJoin) left, rslj, groupId.incrementAndGet(), g);
        } else if (left instanceof Union) {
            collectSOps(sops, (Union) left, rslj, groupId.incrementAndGet(), g);
        } else {
            throw new UnsupportedOperatorException(left);
        }
        
        final TupleExpr right = join.getRightArg();
        
        if (right instanceof StatementPattern) {
            collectSOps(sops, (StatementPattern) right, rslj, g, pg);
        } else if (right instanceof Filter) {
            collectSOps(sops, (Filter) right, rslj, g, pg);
        } else if (right instanceof Join) {
            collectSOps(sops, (Join) right, rslj, g, pg);
        } else if (right instanceof LeftJoin) {
            collectSOps(sops, (LeftJoin) right, rslj, groupId.incrementAndGet(), g);
        } else if (right instanceof Union) {
            collectSOps(sops, (Union) right, rslj, groupId.incrementAndGet(), g);
        } else {
            throw new UnsupportedOperatorException(right);
        }
        
    }
    
    private void collectSOps(final List<SOp> sops, final Union union,
    		final boolean rslj, final int g, final int pg) {
    	final TupleExpr left = union.getLeftArg();
    	final TupleExpr right = union.getRightArg();

    	// just add one Union in the case of a chain of Union operators
    	// a chain of Union operators will always end with a Union of two
    	// non-Unions
    	if (!(left instanceof Union || right instanceof Union))
    		sops.add(new SOp(sopId.incrementAndGet(), union, g, pg, rslj));
    	
    	if (left instanceof Union) {
    		collectSOps(sops, (Union) left, rslj, g, pg);
    	} else {
    		collectSOps(sops, left, rslj, groupId.incrementAndGet(), g);
    	}
    	
    	if (right instanceof Union) {
    		collectSOps(sops, (Union) right, rslj, g, pg);
    	} else {
    		collectSOps(sops, right, rslj, groupId.incrementAndGet(), g);
    	}
    }
    
    private void collectSOps(final List<SOp> sops, final Filter filter, 
            final boolean rslj, final int g, final int pg) {
        
        final ValueExpr ve = filter.getCondition();
        // make a constraint, attach it to the rule
        if (ve != null) {
        	sops.add(new SOp(sopId.incrementAndGet(), filter, g, pg, rslj));
        }
        
        final TupleExpr arg = filter.getArg();

        if (arg instanceof StatementPattern) {
            collectSOps(sops, (StatementPattern) arg, rslj, g, pg);
        } else if (arg instanceof Filter) {
            collectSOps(sops, (Filter) arg, rslj, g, pg);
        } else if (arg instanceof Join) {
            collectSOps(sops, (Join) arg, rslj, g, pg);
        } else if (arg instanceof LeftJoin) {
//            collectSOps(sops, (LeftJoin) arg, rslj, groupId.incrementAndGet(), g);
        	collectSOps(sops, (LeftJoin) arg, rslj, g, pg);
        } else if (arg instanceof SingletonSet) {
        	// do nothing
        } else {
            throw new UnsupportedOperatorException(arg);
        }
        
    }
    
    private void collectSOps(final List<SOp> sops, final StatementPattern sp, 
            final boolean rslj, final int g, final int pg) {
        sops.add(new SOp(sopId.incrementAndGet(), sp, g, pg, rslj));
    }

}
