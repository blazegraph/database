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

    /**
	 * Construct a non-optional union.
	 */
	public UnionNode() {
		
		super(false/*optional*/);
		
	}

	public UnionNode(final boolean optional) {
		
		super(optional);
		
	}

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
	
//	public String toString(final int indent) {
//		
//		final String _indent = indent(indent);
//		
//		final StringBuilder sb = new StringBuilder();
//
//        boolean first = true;
//
//        for (IQueryNode n : this) {
//
//            if (!(n instanceof JoinGroupNode)) {
//        
//                continue;
//                
//            }
//
//            if (first) {
//
//                first = false;
//                
//            } else {
//                
//                sb.append(_indent).append("union\n");
//                
//            }
//
//            sb.append(((JoinGroupNode) n).toString(indent)).append("\n");
//
//        }
//
//        return sb.toString();
//
//    }

}
