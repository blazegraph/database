package com.bigdata.bop.engine;

import com.bigdata.bop.BindingSetPipelineOp;

/**
 * A query declaration.
 */
public interface IQueryDecl {

    /**
     * The proxy for the query controller.
     */
    IQueryClient getQueryController();

    /**
     * The query identifier.
     */
    long getQueryId();

    /**
     * The query.
     */
    BindingSetPipelineOp getQuery();

}
