package com.bigdata.rdf.sparql.ast.service;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;

/**
 * Service invocation interface for an in-process service which knows how to
 * process {@link IV}s.
 * 
 * @see ServiceRegistry
 * @see ServiceFactory
 */
public interface BigdataServiceCall extends ServiceCall<IBindingSet> {

}
