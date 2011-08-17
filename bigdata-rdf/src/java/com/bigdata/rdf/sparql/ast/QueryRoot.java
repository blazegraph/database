package com.bigdata.rdf.sparql.ast;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;

/**
 * Contains the operator tree and query metadata (distinct, order by, slice,
 * projection).
 * 
 * @see ProjectionNode
 * @see GroupByNode
 * @see HavingNode
 * @see OrderByNode
 * @see SliceNode
 * 
 *      FIXME AssigmentNodes => PROJECT(distinct:boolean, value expression list)
 *      plus isWildcard(), isAggregate().
 * 
 *      FIXME GROUP VALUE value expression list (AST2BOp must choose the right
 *      group by operator).
 * 
 *      FIXME HAVING value expression list.
 * 
 *      FIXME ORDER BY (value expression, ASC|DSC) list
 * 
 *      FIXME offset/limit => {@link SliceNode}.
 */
public class QueryRoot {

	private final IGroupNode root;
	
    /**
     * The data set is global to the query (it can not be overrriden in a
     * subquery).
     */
	private DatasetNode dataset;
	
    @Deprecated
	private final List<OrderByExpr> orderBy;

	@Deprecated
	private boolean distinct = false;
	
    @Deprecated
	private final List<AssignmentNode> projection;
	
    @Deprecated
	private long offset = 0;

    @Deprecated
	private long limit = Long.MAX_VALUE;
	
	public QueryRoot(final IGroupNode root) {

		this.root = root;
		
		this.orderBy = new LinkedList<OrderByExpr>();
		
		this.projection = new ArrayList<AssignmentNode>();
		
	}

    @Deprecated
	public void addProjectionVar(final VarNode var) {
		projection.add(new AssignmentNode(var,var));
	}
	
    @Deprecated
	public void addProjectionExpression(final AssignmentNode assignment) {
	    projection.add(assignment);
	}
	
    @Deprecated
	public List<AssignmentNode> getProjection() {
		return projection;
	}

    /**
     * FIXME This should just be the ordered (and possibly immutable) list of
     * value expressions. It is Ok if we require a BIND(var,var) so that
     * everything in this list looks like an assignment rather than a simple
     * variable. The caller can be responsible for extracting just the variables
     * if that is what they need (a striterator resolver would do that nicely).
     */
    @Deprecated
	public List<AssignmentNode> getAssignmentProjections(){
        
        final ArrayList<AssignmentNode> assignments = new ArrayList<AssignmentNode>(
                projection.size());

        for (AssignmentNode n : projection) {

	        if(n.getValueExpressionNode().equals(n.getVarNode()))
	            continue;
	        
	        assignments.add(n);
	        
	    }
	    
	    return assignments;
	    
	}
	
    /**
     * Return the ordered array of the variable names for the projected value
     * expressions.
     */
    @Deprecated
	public IVariable[] getProjectionVars() {

        ArrayList<IVariable<IV>> vars = new ArrayList<IVariable<IV>>(
                projection.size());
	    
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

    @Deprecated
	public void addOrderBy(final OrderByExpr orderBy) {
		
		if (this.orderBy.contains(orderBy)) {
			throw new IllegalArgumentException("duplicate");
		}
		
		this.orderBy.add(orderBy);
		
	}
	
    @Deprecated
	public void removeOrderBy(final OrderByExpr orderBy) {
		this.orderBy.remove(orderBy);
	}
	
    @Deprecated
	public boolean hasOrderBy() {
		return orderBy.size() > 0;
	}
	
    @Deprecated
	public List<OrderByExpr> getOrderBy() {
		return Collections.unmodifiableList(orderBy);
	}
	
	@Deprecated
	public void setDistinct(final boolean distinct) {
		this.distinct = distinct;
	}

    @Deprecated
	public boolean isDistinct() {
		return distinct;
	}

    @Deprecated
	public void setOffset(final long offset) {
		this.offset = offset;
	}

    @Deprecated
	public long getOffset() {
		return offset;
	}

    @Deprecated
	public void setLimit(final long limit) {
		this.limit = limit;
	}

    @Deprecated
	public long getLimit() {
		return limit;
	}
	
    @Deprecated
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
			for (OrderByExpr o : orderBy) {
				sb.append(o).append(" ");
			}
			sb.setLength(sb.length()-1);
		}
		
		
		return sb.toString();
		
	}

}
