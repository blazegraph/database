package com.bigdata.rdf.sail.sop;

import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

import com.bigdata.bop.BOp;

public class SOp {

	private int id;
	
    private QueryModelNode op;
    
    private BOp bop;
    
    private int group, parent;
    
    private boolean rslj;
    
	public SOp(final int id, final QueryModelNode op, 
			final int group, final int parent, final boolean rslj) {
		this.id = id;
		this.op = op;
		this.group = group;
		this.parent = parent;
		this.rslj = rslj;
	}
	
	public int getSOpId() {
		return id;
	}
	
	public QueryModelNode getOperator() {
		return op;
	}
	
    public int getGroup() {
        return group;
    }
    
    public int getParentGroup() {
        return parent;
    }
    
    public boolean isRightSideLeftJoin() {
        return rslj;
    }
    
    public void setBOp(final BOp bop) {
    	this.bop = bop;
    }
    
    public BOp getBOp() {
    	return bop;
    }
    
    public String toString() {

        StringBuilder sb = new StringBuilder();
        
        sb.append("SOp: id=").append(id);
        sb.append(", rslj=").append(rslj);
        sb.append(", group=").append(group);
        sb.append(", parent=").append(parent);
        sb.append(", op=");
        if (op instanceof StatementPattern) {
        	final StatementPattern sp = (StatementPattern) op;
        	sb.append("(");
            sb.append(toString(sp.getSubjectVar())).append(" ");
            sb.append(toString(sp.getPredicateVar())).append(" ");
            sb.append(toString(sp.getObjectVar()));
            sb.append(")");
        } else {
        	String s = op.toString().replaceAll("\n", " ");
        	while (s.contains("  ")) {
        		s = s.replaceAll("  ", " ");
        	}
        	sb.append(s);       	
        }
        
        return sb.toString();
        
    }
    
    private String toString(final Var v) {
    	if (v.hasValue()) {
    		final String s = v.getValue().stringValue();
    		final int i = s.indexOf('#');
    		return i >= 0 ? s.substring(s.indexOf('#')) : s;
    	} else {
    		return "?"+v.getName();
    	}
    }
    
}
