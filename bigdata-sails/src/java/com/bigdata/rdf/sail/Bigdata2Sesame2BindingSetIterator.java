package com.bigdata.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryInterruptedException;
import org.openrdf.query.impl.MapBindingSet;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.engine.QueryTimeoutException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.striterator.ICloseableIterator;
import com.bigdata.util.InnerCause;

/**
 * Converts a bigdata {@link ICloseableIterator} {@link IBindingSet}s containing
 * either (a) {@link BigdataValue}s or (b) {@link IV}s having cached
 * {@link BigdataValue}s into a Sesame 2 {@link CloseableIteration} visiting
 * Sesame 2 {@link BindingSet}s containing {@link BigdataValue}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: Bigdata2Sesame2BindingSetIterator.java 5021 2011-08-04
 *          15:44:21Z thompsonbry $
 * @param <E>
 *            The generic type of the thrown exception.
 */
public class Bigdata2Sesame2BindingSetIterator implements
        CloseableIteration<BindingSet, QueryEvaluationException> {

    final protected static Logger log = Logger
            .getLogger(Bigdata2Sesame2BindingSetIterator.class);

    /**
     * The source iterator (will be closed when this iterator is closed).
     */
    private final ICloseableIterator<IBindingSet> src;
    
    private final BindingSet constants;
    
    private boolean open = true;

    /**
     * 
     * @param src
     *            The source iterator (will be closed when this iterator is
     *            closed). All bound values in the visited {@link IBindingSet}s
     *            MUST be either (a) {@link BigdataValue}s -or- (b) {@link IV}s
     *            having a cached {@link BigdataValue}.
     */
    public Bigdata2Sesame2BindingSetIterator(
            final ICloseableIterator<IBindingSet> src) {

        this(src, null);
    }

    /**
     * 
     * @param src
     *            The source iterator (will be closed when this iterator is
     *            closed). All bound values in the visited {@link IBindingSet}s
     *            MUST be {@link IV}s and those {@link IV}s MUST have their
     *            {@link BigdataValue}s cached.
     * @param constants
     *            Optional constants to be united with the output solutions.
     */
    public Bigdata2Sesame2BindingSetIterator(
            final ICloseableIterator<IBindingSet> src,
            final BindingSet constants) {

        if (src == null)
            throw new IllegalArgumentException();

        this.src = src;
        
        this.constants = constants;

    }
    
    /**
     * {@inheritDoc}
     * 
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/503">
     *      Bigdata2Sesame2BindingSetIterator throws QueryEvaluationException
     *      were it should throw NoSuchElementException. </a>
     */
    public boolean hasNext() throws QueryEvaluationException {

        try {
        
            if (open && src.hasNext())
                return true;

            close();

            return false;

        } catch (Throwable t) {

            if (InnerCause.isInnerCause(t, QueryTimeoutException.class)) {
            
                /*
                 * Align with the openrdf API.
                 */
            
                throw new QueryInterruptedException(t);
                
            } else {
                
                throw new QueryEvaluationException(t);
                
            }
            
        }

    }

    public BindingSet next() throws QueryEvaluationException {

        try {
            
            if (!hasNext())
                throw new NoSuchElementException();

            return getBindingSet(src.next());
            
        } catch (NoSuchElementException t) {

            /*
             * Note: This exception should not be wrapped per the ticket above.
             */

            throw t;
            
        } catch (Throwable t) {

            if (InnerCause.isInnerCause(t, QueryTimeoutException.class)) {

                /*
                 * Align with the openrdf API.
                 */

                throw new QueryInterruptedException(t);

            } else {

                throw new QueryEvaluationException(t);

            }

        }

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
    @SuppressWarnings("rawtypes")
    protected BindingSet getBindingSet(final IBindingSet src) {

        if (src == null)
            throw new IllegalArgumentException();

        final int n = src.size();

        final MapBindingSet bindingSet = new MapBindingSet(n /* capacity */);

        final Iterator<Map.Entry<IVariable,IConstant>> itr = src.iterator();

        while(itr.hasNext()) {

            final Map.Entry<IVariable, IConstant> entry = itr.next();

            final IVariable<?> v = entry.getKey();

            final IConstant<?> c = entry.getValue();
            
            final Object val = c.get();

            final BigdataValue value;
            if (val instanceof IV) {
                /*
                 * The bound value is an IV. The IV MUST have the BigdataValue
                 * cached.
                 */
                value = ((IV) val).getValue();
            } else {
                // Otherwise the bound value must be a BigdataValue.
                value = (BigdataValue) val;
            }
            
            bindingSet.addBinding(v.getName(), value);
            
        }
        
        if (constants != null) {
            
            final Iterator<Binding> it = constants.iterator();

            while (it.hasNext()) {
            
                final Binding b = it.next();
                
                bindingSet.addBinding(b.getName(), b.getValue());
                
            }
            
        }
        
        return bindingSet;
        
    }
    
    /**
     * @throws UnsupportedOperationException
     */
    public void remove() throws QueryEvaluationException {

        throw new UnsupportedOperationException();

    }

    public void close() throws QueryEvaluationException {

        if(open) {

            open = false;
            
            src.close();
            
        }
        
    }

}
