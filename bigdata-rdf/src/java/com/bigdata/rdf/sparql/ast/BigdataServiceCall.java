package com.bigdata.rdf.sparql.ast;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Service invocation interface for an in-process service which knows how to
 * process {@link IV}s.
 * 
 * @see ServiceRegistry
 * @see ServiceFactory
 */
public interface BigdataServiceCall extends ServiceCall {

    /**
     * Invoke an in-process, {@link IV} aware service.
     * 
     * @param bindingSets
     *            The BindingsClause from the SPARQL grammar.
     * 
     * @return An iterator from which the solutions can be drained. If the
     *         iterator is closed, the service invocation must be cancelled.
     */
    ICloseableIterator<IBindingSet> call(IBindingSet[] bindingSets);

}
