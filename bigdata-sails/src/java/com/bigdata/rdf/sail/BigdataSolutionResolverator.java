package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Efficiently resolve term identifiers in Bigdata {@link ISolution}s to RDF
 * {@link BigdataValue}s in Sesame 2 {@link BindingSet}s.
 * 
 * @todo The resolution of term identifiers to terms should happen during
 *       asynchronous read-ahead for even better performance (less latency).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * FIXME refactor as {@link IChunkedIterator} and alignment class converting
 * {@link IChunkedIterator} to {@link CloseableIteration}. We should probably
 * also refactor the {@link BigdataStatementIteratorImpl} in the same manner.
 * The refactored classes belong in the bigdata-rdf package since they can be
 * used outside of the SAIL context.
 */
public class BigdataSolutionResolverator implements CloseableIteration<BindingSet, SailException>
{

    final protected static Logger log = Logger.getLogger(BigdataSolutionResolverator.class);

    /**
     * The database whose lexicon will be used to resolve term identifiers to
     * terms.
     */
    private final AbstractTripleStore db;
    
    /**
     * The source iterator (will be closed when this iterator is closed).
     */
    private final IChunkedOrderedIterator<ISolution> src;
    
    /**
     * The index of the last entry returned in the current {@link #chunk} and
     * <code>-1</code> until the first entry is returned.
     */
    private int lastIndex = -1;
    
    /**
     * The current chunk from the source iterator and initially <code>null</code>.
     */
    private ISolution[] chunk = null;

    /**
     * The map that will be used to resolve term identifiers to terms for the
     * current {@link #chunk} and initially <code>null</code>.
     */
    private Map<Long, BigdataValue> terms = null;
    
    /**
     * The elapsed time spend in this class.
     */
    private long elapsed = 0L;
    
    /**
     * 
     * @param db
     *            Used to resolve term identifiers to {@link Value} objects.
     * @param src
     *            The source iterator (will be closed when this iterator is
     *            closed).
     */
    public BigdataSolutionResolverator(AbstractTripleStore db,
            IChunkedOrderedIterator<ISolution> src) {

        if (db == null)
            throw new IllegalArgumentException();

        if (src == null)
            throw new IllegalArgumentException();

        this.db = db;
        
        this.src = src;

        if(log.isDebugEnabled()) {
            
            log.debug("Resolving solutions: db="+db+", src="+src);
            
        }
        
    }
    
    public boolean hasNext() {

        if (lastIndex != -1 && lastIndex + 1 < chunk.length) {
            
            return true;
            
        }
        
        if(log.isDebugEnabled()) {
            
            log.debug("Testing source iterator.");
            
        }
        
        final long begin = System.currentTimeMillis();
        
        try {

            return src.hasNext();
            
        } finally {

            final long tmp = System.currentTimeMillis() - begin;
            elapsed += (tmp);
            if (log.isInfoEnabled())
                log.info("source hasNext: time=" + tmp + "ms");

        }
        
    }

    public BindingSet next() {

        if (!hasNext())
            throw new NoSuchElementException();

        if (lastIndex == -1 || lastIndex + 1 == chunk.length) {

            final long begin = System.currentTimeMillis();
            
            if (log.isInfoEnabled())
                log.info("Fetching next chunk");
            
            // fetch the next chunk of SPOs.
            chunk = src.nextChunk();

            if (log.isInfoEnabled())
                log.info("Fetched chunk: size=" + chunk.length);

            /*
             * Create a collection of the distinct term identifiers used in this
             * chunk.
             */

            final Collection<Long> ids = new HashSet<Long>(chunk.length * 4);

            for (ISolution solution : chunk) {

                final IBindingSet bindingSet = solution.getBindingSet();
                
                assert bindingSet != null;
                
                final Iterator<Map.Entry<IVariable,IConstant>> itr = bindingSet.iterator();

                while(itr.hasNext()) {

                    final Map.Entry<IVariable,IConstant> entry = itr.next();
                    
                    final Long termId = (Long) entry.getValue().get();

                    ids.add(termId);
                    
                }

            }

            if (log.isInfoEnabled())
                log.info("Resolving " + ids.size() + " term identifiers");
            
            // batch resolve term identifiers to terms.
            terms = db.getLexiconRelation().getTerms(ids);

            if (log.isInfoEnabled())
                log.info("Resolved " + ids.size() + " term identifiers");
            
            // reset the index.
            lastIndex = 0;
            
            final long tmp = System.currentTimeMillis() - begin;
            elapsed += (tmp);
            if(log.isInfoEnabled()) log.info("nextChunk ready: time="+tmp+"ms");
                        
        } else {
            
            // index of the next SPO in this chunk.
            lastIndex++;
            
        }

        if(log.isDebugEnabled()) {
            
            log.debug("lastIndex=" + lastIndex + ", chunk.length="
                    + chunk.length);
            
        }
        
        // the current solution
        final ISolution solution = chunk[lastIndex];

        // resolve the solution to a Sesame2 BindingSet
        final BindingSet bindingSet = getBindingSet(solution);
                
        if (log.isDebugEnabled()) {
            
            log.debug("solution=" + solution);

            log.debug("bindingSet=" + bindingSet);

        }

        return bindingSet;

    }

    /**
     * Resolve the term identifiers in the {@link ISolution} using the map
     * populated when we fetched the current chunk and return a Sesame2
     * {@link BindingSet} populated with the corresponding
     * {@link BigdataValue}s.
     * 
     * @param solution
     *            A solution for a {@link TupleExpr} whose bindings are term
     *            identifiers.
     *            
     * @return The corresponding Sesame 2 {@link BindingSet} in which the
     *         term identifiers have been resolved to {@link BigdataValue}s.
     */
    private BindingSet getBindingSet(ISolution solution) {

        assert solution != null;

        final IBindingSet sourceBindingSet = solution.getBindingSet();
        
        assert sourceBindingSet != null;
        
        final int n = sourceBindingSet.size();

        final MapBindingSet bindingSet = new MapBindingSet(n /* capacity */);

        final Iterator<Map.Entry<IVariable,IConstant>> itr = sourceBindingSet.iterator();

        while(itr.hasNext()) {

            final Map.Entry<IVariable,IConstant> entry = itr.next();
            
            final Long termId = (Long)entry.getValue().get();
            
            final BigdataValue value = terms.get(termId);

            if (value == null)
                throw new RuntimeException("Could not resolve termId="
                        + termId);
            
            bindingSet.addBinding(entry.getKey().getName(), value);
            
        }

        // verify all bindings are present.
        assert bindingSet.size() == n;
        
        return bindingSet;
        
    }
    
    /**
     * @throws UnsupportedOperationException
     */
    public void remove() {

        throw new UnsupportedOperationException();

    }

    public void close() {

        if (log.isInfoEnabled())
            log.info("elapsed=" + elapsed);
        
        src.close();

        chunk = null;
        
        terms = null;
        
    }

}
