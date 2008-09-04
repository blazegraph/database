package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Aligns bigdata {@link ICloseableIterator} {@link IBindingSet}s containing
 * {@link BigdataValue}s with a Sesame 2 {@link CloseableIteration} visiting
 * Sesame 2 {@link BindingSet}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type of the thrown exception.
 */
public class Bigdata2Sesame2BindingSetIterator<E extends Exception> implements
        CloseableIteration<BindingSet, E> {

    final protected static Logger log = Logger
            .getLogger(Bigdata2Sesame2BindingSetIterator.class);

    /**
     * The source iterator (will be closed when this iterator is closed).
     */
    private final ICloseableIterator<IBindingSet> src;
    
    /**
     * 
     * @param src
     *            The source iterator (will be closed when this iterator is
     *            closed). All bound values in the visited {@link IBindingSet}s
     *            MUST be {@link BigdataValue}s.
     */
    public Bigdata2Sesame2BindingSetIterator(ICloseableIterator<IBindingSet> src) {

        if (src == null)
            throw new IllegalArgumentException();

        this.src = src;

    }
    
    public boolean hasNext() throws E {

        return src.hasNext();
        
    }

    public BindingSet next() throws E {

        if (!hasNext())
            throw new NoSuchElementException();

        return getBindingSet(src.next());

    }

    /**
     * Aligns a bigdata {@link IBindingSet} with the Sesame 2 {@link BindingSet}.
     * 
     * @param sourceBindingSet
     *            A bigdata {@link IBindingSet} containing only
     *            {@link BigdataValue}s.
     * 
     * @return The corresponding Sesame 2 {@link BindingSet}.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws ClassCastException
     *             if a bound value is not a {@link BigdataValue}.
     */
    protected BindingSet getBindingSet(IBindingSet sourceBindingSet) {

        if (sourceBindingSet == null)
            throw new IllegalArgumentException();
        
        final int n = sourceBindingSet.size();

        final MapBindingSet bindingSet = new MapBindingSet(n /* capacity */);

        final Iterator<Map.Entry<IVariable,IConstant>> itr = sourceBindingSet.iterator();

        while(itr.hasNext()) {

            final Map.Entry<IVariable, IConstant> entry = itr.next();

            bindingSet.addBinding(entry.getKey().getName(),
                    (BigdataValue) entry.getValue().get());
            
        }
        
        return bindingSet;
        
    }
    
    /**
     * @throws UnsupportedOperationException
     */
    public void remove() throws E {

        throw new UnsupportedOperationException();

    }

    public void close() throws E {

        src.close();
        
    }

}
