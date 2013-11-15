package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.rdf.sparql.ast.optimizers.StaticOptimizer;
import com.bigdata.rdf.store.ITripleStore;

/**
 * A special kind of group {@link IGroupNode} that represents the sparql union
 * operator.
 * <p>
 * Note: This node only accepts {@link JoinGroupNode}s as children.
 */
public class UnionNode extends GraphPatternGroup<JoinGroupNode>  implements IReorderableNode {
	
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private static final transient Logger log = Logger.getLogger(UnionNode.class);
	
    /**
     * Required deep copy constructor.
     */
    public UnionNode(UnionNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public UnionNode(BOp[] args, Map<String, Object> anns) {

        super(args, anns);
        for (BOp x:args) {
        	assert x instanceof JoinGroupNode;
        }

    }

	//    /**
//	 * Construct a non-optional union.
//	 */
	public UnionNode() {
		
	}

//	public UnionNode(final boolean optional) {
//		
//		super(optional);
//		
//	}

    @Override
    public UnionNode addChild(final JoinGroupNode child) {

        final JoinGroupNode group = (JoinGroupNode) child;

        // can only add non-optional join groups as children to union
        if (group.isOptional()) {

            log.warn("optional tag on child will be ignored");

        }

        return (UnionNode) super.addChild(child);

    }

    /**
     * Returns <code>false</code>.
     */
    final public boolean isOptional() {
        
        return false;
        
    }

    /**
     * Returns <code>false</code>.
     */
    final public boolean isMinus() {
     
        return false;
        
    }
    

	@Override
 	public long getEstimatedCardinality(StaticOptimizer optimizer, ITripleStore db) {
 		long cardinality = 0;
 		for (JoinGroupNode child : this) {
 			StaticOptimizer opt = new StaticOptimizer(optimizer, child.getReorderableChildren(db));
 			cardinality += opt.getCardinality();
 		}
 		return cardinality;
 	}

	@Override
	public boolean isReorderable(ITripleStore db) {
		for (JoinGroupNode child : this) {
			for (IGroupMemberNode grandchild : child) {
				if (! (grandchild instanceof IReorderableNode))
					return false;
				if (! ((IReorderableNode)grandchild).isReorderable(db))
					return false;
			}
		}
		return true;
	}


    @Override
    public void addArg(final BOp newArg) {
    	assert newArg instanceof JoinGroupNode;
        super.addArg(newArg);
    }

    @Override
    public void addArg(final int index, final BOp newArg) {
    	assert newArg instanceof JoinGroupNode;
        super.addArg(index, newArg);

    }

    @Override
    public int replaceWith(final BOp oldChild, final BOp newChild) {
    	assert newChild instanceof JoinGroupNode;
    	return  super.replaceWith(oldChild, newChild);
    }
}
