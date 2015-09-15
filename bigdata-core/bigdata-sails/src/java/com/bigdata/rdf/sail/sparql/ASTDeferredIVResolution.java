package com.bigdata.rdf.sail.sparql;

import org.openrdf.query.MalformedQueryException;

import com.bigdata.rdf.sail.sparql.ast.ASTQueryContainer;
import com.bigdata.rdf.sail.sparql.ast.VisitorException;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.DatasetNode;
import com.bigdata.rdf.sparql.ast.IDataSetNode;
import com.bigdata.rdf.sparql.ast.QueryNodeWithBindingSet;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.Update;
import com.bigdata.rdf.sparql.ast.UpdateRoot;
import com.bigdata.rdf.sparql.ast.ASTContainer.Annotations;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.optimizers.ASTSetValueExpressionsOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTUnresolvedTermsOptimizer;
import com.bigdata.rdf.store.AbstractTripleStore;

public class ASTDeferredIVResolution {
    public static void preEvaluate(final AbstractTripleStore store, final ASTContainer ast) throws MalformedQueryException {

        final QueryRoot queryRoot = (QueryRoot)ast.getProperty(Annotations.ORIGINAL_AST);
        final ASTQueryContainer qc = (ASTQueryContainer)ast.getProperty(Annotations.PARSE_TREE);
        
        final BatchRDFValueResolver resolver = new BatchRDFValueResolver(true/* readOnly */);
        
        final BigdataASTContext context = new BigdataASTContext(store);
        resolver.processOnPrepareEvaluate(queryRoot, context);

        /*
         * Handle dataset declaration
         * 
         * Note: Filters can be attached in order to impose ACLs on the
         * query. This has to be done at the application layer at this
         * point, but it might be possible to extend the grammar for this.
         * The SPARQL end point would have to be protected from external
         * access if this were done. Perhaps the better way to do this is to
         * have the NanoSparqlServer impose the ACL filters. There also
         * needs to be an authenticated identity to make this work and that
         * could be done via an integration within the NanoSparqlServer web
         * application container.
         * 
         * Note: This handles VIRTUAL GRAPH resolution.
         */
        if (qc!=null && qc.getOperation()!=null) {
            final DatasetNode dataSetNode = new DatasetDeclProcessor(context)
                .process(qc.getOperation().getDatasetClauseList(), false);
    
            if (dataSetNode != null) {
    
                queryRoot.setDataset(dataSetNode);
    
            }
        }
        
        /*
         * I think here we could set the value expressions and do last- minute
         * validation.
         */
        final ASTSetValueExpressionsOptimizer opt = new ASTSetValueExpressionsOptimizer();

        final AST2BOpContext context2 = new AST2BOpContext(ast, context.tripleStore);

        final QueryRoot queryRoot2 = (QueryRoot) opt.optimize(context2, new QueryNodeWithBindingSet(queryRoot, null)).getQueryNode();

        try {

            BigdataExprBuilder.verifyAggregate(queryRoot2);

        } catch (final VisitorException e) {

            throw new MalformedQueryException(e.getMessage(), e);

        }
        
        final ASTUnresolvedTermsOptimizer termsResolver = new ASTUnresolvedTermsOptimizer();
        final QueryRoot queryRoot3 = (QueryRoot) termsResolver.optimize(context2, new QueryNodeWithBindingSet(queryRoot, null)).getQueryNode();
        
        queryRoot3.setPrefixDecls(ast.getOriginalAST().getPrefixDecls());
        ast.setOriginalAST(queryRoot3);
    }

    public static void preUpdate(final AbstractTripleStore store, final ASTContainer ast) throws MalformedQueryException {

        final UpdateRoot qc = (UpdateRoot)ast.getProperty(Annotations.ORIGINAL_AST);
        
        /*
         * Handle dataset declaration. It only appears for DELETE/INSERT
         * (aka ASTModify). It is attached to each DeleteInsertNode for
         * which it is given.
         */
        for (final Update update: qc.getChildren()) {
            final DatasetNode dataSetNode = new DatasetDeclProcessor(new BigdataASTContext(store))
                .process(update.getDatasetClauses(), true);
            
                if (dataSetNode != null) {
        
                        /*
                         * Attach the data set (if present)
                         * 
                         * Note: The data set can only be attached to a
                         * DELETE/INSERT operation in SPARQL 1.1 UPDATE.
                         */
        
                        ((IDataSetNode) update).setDataset(dataSetNode);
        
                }
        }
        final AST2BOpContext context2 = new AST2BOpContext(ast, store);
        final ASTUnresolvedTermsOptimizer termsResolver = new ASTUnresolvedTermsOptimizer();
        final UpdateRoot queryRoot3 = (UpdateRoot) termsResolver.optimize(context2, new QueryNodeWithBindingSet(qc, null)).getQueryNode();
        if (ast.getOriginalUpdateAST().getPrefixDecls()!=null && !ast.getOriginalUpdateAST().getPrefixDecls().isEmpty()) {
            queryRoot3.setPrefixDecls(ast.getOriginalUpdateAST().getPrefixDecls());
        }
        ast.setOriginalUpdateAST(queryRoot3);

    }

}
