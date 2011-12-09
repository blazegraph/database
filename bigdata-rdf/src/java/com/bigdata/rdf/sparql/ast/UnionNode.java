package com.bigdata.rdf.sparql.ast;

import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;

/**
 * A special kind of group {@link IGroupNode} that represents the sparql union
 * operator.
 * <p>
 * Note: This node only accepts {@link JoinGroupNode}s as children.
 */
public class UnionNode extends GraphPatternGroup<JoinGroupNode> {
	
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

        // can only add non-optional join groups as children to union
        if (!(child instanceof JoinGroupNode)) {

            throw new IllegalArgumentException("UnionNode only permits "
                    + JoinGroupNode.class.getSimpleName()
                    + " children, but child=" + child);

        }

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
    
}
