package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.TermId;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataValueIterator;
import com.bigdata.rdf.store.BigdataValueIteratorImpl;
import com.bigdata.search.IHit;
import com.bigdata.striterator.ChunkedWrappedIterator;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * Converts the term identifiers from a text search into variable bindings.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HitConvertor implements
        CloseableIteration<BindingSet, QueryEvaluationException> {

    final private AbstractTripleStore database;

    final private BigdataValueIterator src;

    final private Var svar;

    final private BindingSet bindings;
    
    final private Set<URI> graphs;
    
    private BigdataValue next;
    
    @SuppressWarnings("unchecked")
    public HitConvertor(final AbstractTripleStore database,
            final Iterator<IHit> src, final Var svar, final BindingSet bindings) {
        
        this(database, src, svar, bindings, null);
        
    }

    @SuppressWarnings("unchecked")
    public HitConvertor(final AbstractTripleStore database,
            final Iterator<IHit> src, final Var svar, final BindingSet bindings,
            Set<URI> graphs) {

        this.database = database;

        /*
         * Resolve the document identifier from the hit (the term identifers are
         * treated as "documents" by the search engine).
         * 
         * And then wrap up the term identifier iterator as a chunked iterator.
         * 
         * And finally wrap up the chunked term identifier iterator with an
         * iterator that efficiently resolves term identifiers to BigdataValue
         * objects that we can pass along to Sesame.
         */
        this.src = new BigdataValueIteratorImpl(database,
                new ChunkedWrappedIterator<IV>(new Striterator(src)
                        .addFilter(new Resolver() {

                            private static final long serialVersionUID = 1L;

                            @Override
                            protected Object resolve(final Object arg0) {

                                final IHit hit = (IHit) arg0;

                                return new TermId(hit.getDocId());
                            }

                        })));

        this.svar = svar;

        this.bindings = bindings;
        
        this.graphs = graphs;

    }

    /**
     * Return <code>true</code> iff the element should be visited.  If in 
     * quads mode, the bigdata value must appear in a statement in the named
     * graphs if there are any.
     * 
     * @param value
     *            The value.
     */
    protected boolean isValid(BigdataValue value) {
        
        if (graphs != null) {
            // check each graph to see if the literal appears in a statement
            for (URI uri : graphs) {
                if (database.getAccessPath(
                        null, null, value, uri).rangeCount(true) > 0) {
                    return true;
                }
            }
            return false;
        }
        
        return true;
        
    }
    
    public void close() throws QueryEvaluationException {

        src.close();
        
    }

    public boolean hasNext() throws QueryEvaluationException {

        if (next != null)
            return true;
            
        while(src.hasNext()) {
            
            final BigdataValue e = src.next();
            
            if(isValid(e)) {
                
                next = e;
                
                return true;
                
            }
            
        }
        
        return false;

    }
    
    
    /**
     * Binds the next {@link BigdataValue} (must be a Literal).
     * 
     * @throws QueryEvaluationException
     */
    public BindingSet next() throws QueryEvaluationException {

        if (!hasNext())
            throw new NoSuchElementException();

        final BigdataValue val = next;

        next = null;
        
        final QueryBindingSet result = new QueryBindingSet(bindings);

        if (!(val instanceof Literal)) {

            throw new QueryEvaluationException("Not a literal? : " + val);

        }

        /*
         * Note: Hopefully nothing will choke when we bind a Literal to a
         * variable that appears in the subject position!
         */

        result.addBinding(svar.getName(), val);

        return result;

    }

    public void remove() throws QueryEvaluationException {

        throw new UnsupportedOperationException();

    }

}
