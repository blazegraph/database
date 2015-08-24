package com.bigdata.rdf.sparql.ast;

/**
 * Exception indicating that a quads operation such as an update or extract
 * on a named graph is issued, but the database is bootstrapped in triples mode
 * only. This exception might be thrown in the static analysis phase when
 * encountering constructs such as NAMED, WITH, GRAPH, etc, or at runtime.
 * 
 * @see <a href="http://trac.blazegraph.com/ticket/653">Slow query with bind</a>
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 */
public class QuadsOperationInTriplesModeException extends RuntimeException {

   private static final long serialVersionUID = -472504247432552941L;

   public QuadsOperationInTriplesModeException() {
      super();
   };

   public QuadsOperationInTriplesModeException(String msg) {
      super(msg);
   };

   
}
