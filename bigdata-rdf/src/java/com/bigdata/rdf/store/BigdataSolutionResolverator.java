package com.bigdata.rdf.store;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Value;

import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Efficiently resolve term identifiers in Bigdata {@link ISolution}s to RDF
 * {@link BigdataValue}s.
 * 
 * @todo The resolution of term identifiers to terms should happen during
 *       asynchronous read-ahead for even better performance (less latency).
 * 
 * @todo Another approach would be to serialize the term for the object position
 *       into the value in the OSP index. That way we could pre-materialize the
 *       term for some common access patterns.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BigdataStatementIteratorImpl
 */
public class BigdataSolutionResolverator implements ICloseableIterator<IBindingSet>
{

    final protected static Logger log = Logger.getLogger(BigdataSolutionResolverator.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

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
     * The elapsed time for the iterator instance.
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

        if(DEBUG) {
            
            log.debug("Resolving solutions: db="+db+", src="+src);
            
        }
        
    }
    
    public boolean hasNext() {

        if (lastIndex != -1 && lastIndex + 1 < chunk.length) {
            
            return true;
            
        }
        
        if(DEBUG) {
            
            log.debug("Testing source iterator.");
            
        }
        
        final long begin = System.currentTimeMillis();
        
        try {

            return src.hasNext();
            
        } finally {

            final long tmp = System.currentTimeMillis() - begin;
            
            elapsed += (tmp);
            
            if (INFO)
                log.info("source hasNext: time=" + tmp + "ms");

        }
        
    }

    public IBindingSet next() {

        final long begin = System.currentTimeMillis();
        
        if (!hasNext())
            throw new NoSuchElementException();

        if (lastIndex == -1 || lastIndex + 1 == chunk.length) {

            if (INFO)
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

            if (INFO)
                log.info("Resolving " + ids.size() + " term identifiers");
            
            // batch resolve term identifiers to terms.
            terms = db.getLexiconRelation().getTerms(ids);

            // reset the index.
            lastIndex = 0;

            if (INFO)
                log.info("nextChunk ready: time="
                        + (System.currentTimeMillis() - begin) + "ms");
                        
        } else {
            
            // index of the next SPO in this chunk.
            lastIndex++;
            
        }

        if(DEBUG) {
            
            log.debug("lastIndex=" + lastIndex + ", chunk.length="
                    + chunk.length);
            
        }
        
        // the current solution
        final ISolution solution = chunk[lastIndex];

        // resolve the solution to an IBindingSet containing Values (vs termIds)
        final IBindingSet bindingSet = getBindingSet(solution);
                
        if (DEBUG) {
            
            log.debug("solution=" + solution);

            log.debug("bindingSet=" + bindingSet);

        }

        elapsed += (System.currentTimeMillis() - begin);

        return bindingSet;

    }

    /**
     * Resolve the term identifiers in the {@link ISolution} using the map
     * populated when we fetched the current chunk and return the
     * {@link IBindingSet} for that solution in which term identifiers have been
     * resolved to their corresponding {@link BigdataValue}s.
     * 
     * @param solution
     *            A solution whose {@link Long}s will be interepreted as term
     *            identifiers and resolved to the corresponding
     *            {@link BigdataValue}s.
     * 
     * @return The corresponding {@link IBindingSet} in which the term
     *         identifiers have been resolved to {@link BigdataValue}s.
     *         
     * @throws IllegalStateException
     *             if the {@link IBindingSet} was not materialized with the
     *             {@link ISolution}.
     * 
     * @todo this points out a problem where we would be better off strongly
     *       typing the term identifiers with their own class rather than using
     *       {@link Long} since we can not distinguish a {@link Long}
     *       materialized by a join against some non-RDF relation from a
     *       {@link Long} that is a term identifier.
     */
    protected IBindingSet getBindingSet(ISolution solution) {

        assert solution != null;

        final IBindingSet bindingSet = solution.getBindingSet();
        
        if(bindingSet == null) {
            
            throw new IllegalStateException("BindingSet was not materialized");
            
        }
        
        final Iterator<Map.Entry<IVariable,IConstant>> itr = bindingSet.iterator();

        while(itr.hasNext()) {

            final Map.Entry<IVariable,IConstant> entry = itr.next();
            
            final Object boundValue = entry.getValue().get();
            
            if (!(boundValue instanceof Long))
                continue;
            
            final Long termId = (Long)boundValue;
            
            final BigdataValue value = terms.get(termId);

            if (value == null)
                throw new RuntimeException("Could not resolve termId="
                        + termId);
            
            // replace the binding.
            bindingSet.set(entry.getKey(), new Constant<BigdataValue>(
                    value));
            
        }
        
        return bindingSet;

    }
    
    /**
     * @throws UnsupportedOperationException
     */
    public void remove() {

        throw new UnsupportedOperationException();

    }

    public void close() {

        if (INFO)
            log.info("elapsed=" + elapsed);
        
        src.close();

        chunk = null;
        
        terms = null;
        
    }

}
