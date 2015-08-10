package com.bigdata.rdf.sparql.ast;

import java.util.Properties;

import com.bigdata.bop.BOp;
import com.bigdata.bop.PipelineOp;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;
import com.bigdata.rdf.sparql.ast.optimizers.ASTQueryHintOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;

/**
 * This is the basic interface for any AST operator that appears in the query
 * plan.
 */
public interface IQueryNode extends BOp {

	public interface Annotations {
		
        /**
         * An optional {@link Properties} object specifying query hints which
         * apply to <i>this</i> AST node.
         * <p>
         * The relationship between SPARQL level query hints, the annotations on
         * the {@link ASTBase} and {@link PipelineOp} nodes is a bit complex,
         * and this {@link Properties} object is a bit complex. Briefly, this
         * {@link Properties} object contains property values which will be
         * applied (copied) onto generated {@link PipelineOp}s while annotations
         * on the {@link ASTBase} nodes are interpreted by {@link IASTOptimizer}
         * s and {@link AST2BOpUtility}. SPARQL level query hints map into one
         * or another of these things, or may directly set attributes on the
         * {@link AST2BOpContext}.
         * 
         * @see ASTQueryHintOptimizer
         */
        String QUERY_HINTS = "queryHints";
        
	}
	

	/**
	 * A string representation of a recursive structure with pretty-print indent.
	 * @param indent
	 * @return
	 */
	String toString(final int indent);
    
}
