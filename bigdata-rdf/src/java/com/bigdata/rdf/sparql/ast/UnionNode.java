package com.bigdata.rdf.sparql.ast;

import org.apache.log4j.Logger;

/**
 * A special kind of group {@link IGroupNode} that represents the sparql union
 * operator. This node only accepts {@link JoinGroupNode}s as children. 
 */
public class UnionNode extends GroupNodeBase 
		implements IGroupNode, IQueryNode {
	
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

	/**
	 * This method will throw an IllegalArgumentException if you try to add
	 * something other than a JoinGroupNode as a child.
	 */
	@Override
	public IGroupNode addChild(final IQueryNode child) {
		
		// can only add non-optional join groups as children to union
		if (!(child instanceof JoinGroupNode)) {
			throw new IllegalArgumentException();
		}
		
		final JoinGroupNode group = (JoinGroupNode) child;
		
		// can only add non-optional join groups as children to union
		if (group.isOptional()) {
			log.warn("optional tag on child will be ignored");
		}
		
		return super.addChild(child);
		
	}
	
	public String toString() {
		
		return toString(0);
		
	}
	
	public String toString(final int indent) {
		
		final String _indent;
		if (indent <= 0) {
			
			_indent = "";
			
		} else {
			
			final StringBuilder sb = new StringBuilder();
			for (int i = 0; i < indent; i++) {
				sb.append(" ");
			}
			_indent = sb.toString();
			
		}
		
		final StringBuilder sb = new StringBuilder();
		
//		sb.append(_indent).append("union\n");
//		sb.append(_indent).append("{\n");
		
		for (IQueryNode n : this) {
			if (!(n instanceof JoinGroupNode)) {
				continue;
			}
			sb.append(((JoinGroupNode)n).toString(indent)).append("\n");
			
			sb.append(_indent).append("union\n");
		}
		sb.setLength(sb.length()-6-indent-1);
		
//		sb.append(_indent).append("}");
		
		return sb.toString();
		
	}
	
}
