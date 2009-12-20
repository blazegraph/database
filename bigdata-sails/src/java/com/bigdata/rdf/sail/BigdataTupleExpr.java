package com.bigdata.rdf.sail;

import java.util.Set;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.TupleExpr;

public class BigdataTupleExpr implements TupleExpr {
    
    private TupleExpr tupleExpr;
    
    private BigdataNativeEvaluationLevel level;
    
    public BigdataTupleExpr(TupleExpr tupleExpr) {
        this.tupleExpr = tupleExpr;
        this.level = BigdataNativeEvaluationLevel.None;
    }
    
    public BigdataNativeEvaluationLevel getNativeEvaluationLevel() {
        return level;
    }
    
    public void setNativeEvaluationLevel(BigdataNativeEvaluationLevel level) {
        this.level = level;
    }
    
    public TupleExpr clone() {
        return tupleExpr.clone();
    }
    
    public Set<String> getBindingNames() {
        return tupleExpr.getBindingNames();
    }

    public QueryModelNode getParentNode() {
        return tupleExpr.getParentNode();
    }

    public String getSignature() {
        return tupleExpr.getSignature();
    }

    public void replaceChildNode(QueryModelNode arg0, QueryModelNode arg1) {
        tupleExpr.replaceChildNode(arg0, arg1);
    }

    public void replaceWith(QueryModelNode arg0) {
        tupleExpr.replaceWith(arg0);
    }

    public void setParentNode(QueryModelNode arg0) {
        tupleExpr.setParentNode(arg0);
    }

    public <X extends Exception> void visit(QueryModelVisitor<X> arg0) throws X {
        tupleExpr.visit(arg0);
    }

    public <X extends Exception> void visitChildren(QueryModelVisitor<X> arg0)
            throws X {
        tupleExpr.visitChildren(arg0);
    }
}
