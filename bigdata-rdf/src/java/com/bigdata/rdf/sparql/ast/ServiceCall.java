package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.relation.accesspath.IAsynchronousIterator;

public interface ServiceCall {
    /**
     * Evaluate the service call given the running query
     * @param runningQuery
     * @return Iterator over a set of solution binding sets
     */
    public IAsynchronousIterator<IBindingSet[]> call(IRunningQuery runningQuery);
}
