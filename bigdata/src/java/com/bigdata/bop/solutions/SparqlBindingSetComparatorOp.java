package com.bigdata.bop.solutions;

import java.util.Comparator;
import java.util.Map;

import org.openrdf.query.algebra.evaluation.util.ValueComparator;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;

/**
 * A comparator for SPARQL binding sets.
 * 
 * @see http://www.w3.org/TR/rdf-sparql-query/#modOrderBy
 * @see ValueComparator
 * 
 * @todo unit tests.
 */
public class SparqlBindingSetComparatorOp extends ComparatorOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Required deep copy constructor.
     */
    public SparqlBindingSetComparatorOp(final SparqlBindingSetComparatorOp op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public SparqlBindingSetComparatorOp(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

    }
    
    /**
     * @see Annotations#ORDER
     */
    public ISortOrder<?>[] getOrder() {

        return getRequiredProperty(Annotations.ORDER);

    }

    /**
     * The sort order to be imposed.
     */
    private transient ISortOrder<?>[] order;

    private transient Comparator vc;
    
    public int compare(final IBindingSet bs1, final IBindingSet bs2) {

        if (order == null) {

            // lazy initialization.
            order = getOrder();
            
            if (order == null)
                throw new IllegalArgumentException();

            if (order.length == 0)
                throw new IllegalArgumentException();

            // comparator for RDF Value objects.
            vc = new ValueComparator();
            
        }
        
        for (int i = 0; i < order.length; i++) {

            final ISortOrder<?> o = order[i];

            final IVariable v = o.getVariable();

            int ret = vc.compare(bs1.get(v).get(), bs2.get(v).get());

            if (!o.isAscending())
                ret = -ret;

            if (ret != 0) {
                // not equal for this variable.
                return ret;
            }

        }

        // equal for all variables.
        return 0;

    }

}
