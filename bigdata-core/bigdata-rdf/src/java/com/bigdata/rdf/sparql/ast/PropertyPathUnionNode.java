package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import com.bigdata.bop.BOp;

/**
 * This node is purely to trick the ASTJoinOrderByTypeOptimizer - we need to
 * keep the property path stuff in the right order, even the UNIONs.
 */
public class PropertyPathUnionNode extends UnionNode {
	
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Constructor required for {@link com.bigdata.bop.BOpUtility#deepCopy(FilterNode)}.
     */
    public PropertyPathUnionNode(PropertyPathUnionNode op) {

        super(op);
        
    }

    /**
     * Required shallow copy constructor.
     */
    public PropertyPathUnionNode(BOp[] args, Map<String, Object> anns) {

        super(args, anns);

    }

    /**
	 * Construct a non-optional union.
	 */
	public PropertyPathUnionNode() {
	}

}
