package com.bigdata.bop.engine;

import java.util.UUID;

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
    UUID getQueryId();

    /**
     * The query.
     */
    BindingSetPipelineOp getQuery();

}
