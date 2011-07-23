package com.bigdata.rdf.sparql.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.bigdata.bop.IVariable;

import com.bigdata.rdf.internal.IV;

/**
 * Contains the operator tree and query metadata (distinct, order by, slice,
 * projection).
 */
public class QueryRoot {

	private final IGroupNode root;
	
	private DatasetNode dataset;
	
	private final List<OrderByNode> orderBy;
	
	private boolean distinct = false;
	
	private final List<AssignmentNode> projection;
	
	private long offset = 0;
	
	private long limit = Long.MAX_VALUE;
	
	public QueryRoot(final IGroupNode root) {

		this.root = root;
		
		this.orderBy = new LinkedList<OrderByNode>();
		
		this.projection = new ArrayList<AssignmentNode>();
		
	}

	public void addProjectionVar(final VarNode var) {
		projection.add(new AssignmentNode(var,var));
	}
	
	public void addProjectionExpression(final AssignmentNode assignment) {
	    projection.add(assignment);
	}
	
	public List<AssignmentNode> getProjection() {
		return projection;
	}
	
	public List<AssignmentNode> getAssignmentProjections(){
	    ArrayList<AssignmentNode> assignments=new ArrayList<AssignmentNode>();
	    for(AssignmentNode n:projection){
	        if(n.getValueExpressionNode().equals(n.getVarNode()))
	            continue;
	        assignments.add(n);
	    }
	    return assignments;
	}
	
	public IVariable[] getProjectionVars() {
	    ArrayList<IVariable< IV>> vars=new ArrayList<IVariable<IV>>(projection.size());
	    for(AssignmentNode n:projection){
	       vars.add(n.getVar());
	    }
        return (IVariable[]) vars.toArray(new IVariable[vars.size()]);
	}
	
	public IGroupNode getRoot() {
		return root;
	}
	
	public void setDataset(final DatasetNode dataset) {
		this.dataset = dataset;
	}
	
	public DatasetNode getDataset() {
		return dataset;
	}

	public void addOrderBy(final OrderByNode orderBy) {
		
		if (this.orderBy.contains(orderBy)) {
			throw new IllegalArgumentException("duplicate");
		}
		
		this.orderBy.add(orderBy);
		
	}
	
	public void removeOrderBy(final OrderByNode orderBy) {
		this.orderBy.remove(orderBy);
	}
	
	public boolean hasOrderBy() {
		return orderBy.size() > 0;
	}
	
	public List<OrderByNode> getOrderBy() {
		return Collections.unmodifiableList(orderBy);
	}
	
	public void setDistinct(final boolean distinct) {
		this.distinct = distinct;
	}

	public boolean isDistinct() {
		return distinct;
	}

	public void setOffset(final long offset) {
		this.offset = offset;
	}

	public long getOffset() {
		return offset;
	}

	public void setLimit(final long limit) {
		this.limit = limit;
	}

	public long getLimit() {
		return limit;
	}
	
	public boolean hasSlice() {
		return offset > 0 || limit < Long.MAX_VALUE;
	}
	
	public String toString() {
		
		final StringBuilder sb = new StringBuilder();
		
		sb.append("select");
		
		if (distinct) {
			sb.append(" distinct");
		}
		
		if (projection.size() > 0) {
			for (AssignmentNode v : projection) {
				sb.append(v);
			}
		} else {
			sb.append(" *");
		}
		
		sb.append("\nwhere\n");
		sb.append(root.toString());
		
		if (offset > 0l) {
			sb.append("\noffset ").append(offset);
		}
		
		if (limit < Long.MAX_VALUE) {
			sb.append("\nlimit ").append(limit);
		}
		
		if (orderBy.size() > 0) {
			sb.append("\norderBy ");
			for (OrderByNode o : orderBy) {
				sb.append(o).append(" ");
			}
			sb.setLength(sb.length()-1);
		}
		
		
		return sb.toString();
		
	}

}
