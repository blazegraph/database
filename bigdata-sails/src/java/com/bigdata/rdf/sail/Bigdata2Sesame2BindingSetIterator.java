package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.query.impl.MapBindingSet;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.model.BigdataValue;
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
     * @param src
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
    protected BindingSet getBindingSet(final IBindingSet src) {

        if (src == null)
            throw new IllegalArgumentException();

        final int n = src.size();

        final MapBindingSet bindingSet = new MapBindingSet(n /* capacity */);

        final Iterator<Map.Entry<IVariable,IConstant>> itr = src.iterator();

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
