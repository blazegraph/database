package com.bigdata.rdf.sparql.ast;

import org.apache.log4j.Logger;

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
	 * Construct a non-optional union.
	 */
	public UnionNode() {
		
		super(false);
		
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
