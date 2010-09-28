package com.bigdata.bop.ap.filter;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IElement;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.relation.accesspath.IAccessPath;

/**
 * Filter imposes the "same variable" constraint on the elements visited by an
 * {@link IAccessPath}. The filter is required IFF a {@link IVariable} appears
 * in more than one position for the {@link IPredicate} associated with the
 * {@link IAccessPath}. For example, in <code>spo(?g, p1, o1, ?g)</code>, the
 * variable <i>g</i> shows up at both index ZERO (0) and index THREE (3).
 * <p>
 * An instance of the filter is created by passing in an {@link IPredicate}. The
 * filter creates an array of variables which appear more than once and the
 * index at which those variables appear in the predicate. The filter then does
 * the minimum amount of work and just compares the values found in the
 * different slots in which each variable appears for only those variables which
 * appear more than once in the {@link IPredicate}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 *            The generic type of the elements that will be tested by the
 *            filter.
 */
public class SameVariableConstraint<E> implements Externalizable {

    /**
     * The predicate template.
     */
    private IPredicate<E> p;

    /**
     * An array containing one or more records, each of which has the form
     * 
     * <pre>
     * [n,index[0], index[1], ... index[n-1]]
     * </pre>
     * 
     * , where <i>n</i> is the number of occurrences of some variable and
     * <i>index[i]</i>, for <i>i</i> in <code>[0,n-1]</code>, gives each index
     * in the predicate at which that variable appears.
     * <p>
     * For example, given the predicate
     * 
     * <pre>
     * spo(?g,p1,o1,?g)
     * </pre>
     * 
     * the array would be coded as one record
     * 
     * <pre>
     * [2, 0, 3]
     * </pre>
     * 
     * Given the predicate
     * 
     * <pre>
     * foo(?g,?h,?h,?g,4,?h)
     * </pre>
     * 
     * the array would be coded as two records
     * 
     * <pre>
     * [[2, 0, 3], [3, 1, 2, 5]]
     * </pre>
     * 
     * where the records are indicated by the nested square brackets.
     */
    protected int[] indices;

    /**
     * De-serialization constructor.
     */
    public SameVariableConstraint() {

    }

    public SameVariableConstraint(final IPredicate<E> p, final int[] indices) {

        if (p == null)
            throw new IllegalArgumentException();

        if (indices == null)
            throw new IllegalArgumentException();

        this.p = p;

        this.indices = indices;

    }

//    public boolean canAccept(final Object o) {
//        return true;
//    }

    public boolean accept(final E e) {

        int i = 0;
        while (i < indices.length) {

            // the #of slots at which that variable appears.
            final int nslots = indices[i];
            
            assert nslots >= 2 : "i=" + i + ", nslots=" + nslots + ", indices="
                    + Arrays.toString(indices);

            assert i + nslots < indices.length;
            
            i++;
            
            final int firstIndex = indices[i];
            
            @SuppressWarnings("unchecked")
            final IConstant<?> c0 = new Constant(((IElement) e).get(firstIndex));
//            final IConstant<?> c0 = p.get(e, firstIndex);

            for (int j = 0; j < nslots; j++, i++) {

                if (j == firstIndex) {
                    
                    // no need to compare with self.
                    continue;
                    
                }
                
                final int thisIndex = indices[i];
                
                @SuppressWarnings("unchecked")
                final IConstant<?> c1 = new Constant(((IElement) e)
                        .get(thisIndex));
//                final IConstant<?> c1 = p.get(e, thisIndex);

                // same reference (including null).
                if (c0 == c1)
                    continue;

                if (c0 != null && c1 == null) {

                    return false;

                }

                if (c1 == null && c0 != null) {

                    return false;

                }

                if (!c0.equals(c1)) {

                    // different constant.
                    return false;

                }

            }

        }

        return true;

    }

    public String toString() {

        return super.toString() + "{p=" + p + ", indices="
                + Arrays.toString(indices) + "}";

    }
    
    /**
     * The initial version.
     */
    private static final transient byte VERSION0 = 0;

    /**
     * The current version.
     */
    private static final transient byte VERSION = VERSION0;

    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        final short version = in.readByte();

        switch (version) {
        case VERSION0:
            break;
        default:
            throw new UnsupportedOperationException("Unknown version: "
                    + version);
        }

        p = (IPredicate<E>) in.readObject();

        final int len = (int) LongPacker.unpackLong(in);

        indices = new int[len];

        for (int i = 0; i < len; i++) {

            indices[i] = (int) LongPacker.unpackLong(in);

        }

    }

    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeByte(VERSION);

        out.writeObject(p);

        LongPacker.packLong(out, indices.length);

        for (int i = 0; i < indices.length; i++) {

            LongPacker.packLong(out, indices[i]);

        }

    }

    /**
     * The filter is only created and populated for variables which appear more
     * than once in the predicate. If there are no variables which appear more
     * than once, then the filter IS NOT created.
     * 
     * @param p
     *            The predicate.
     * 
     * @param <E>
     *            The generic type of the elements to be tested by the filter.
     * 
     * @return The filter -or- <code>null</code> iff no variables appear more
     *         than once in the {@link IPredicate}.
     */
    public static <E> SameVariableConstraint<E> newInstance(
            final IPredicate<E> p) {

        // #of slots in this predicate.
        final int arity = p.arity();

        // map exists iff variables are used more than once.
        Map<IVariableOrConstant<?>, Integer> vars = null;

        // #of occurrences across all variables which are used more than once.
        int noccurs = 0;

        // #of variables which are reused within the predicate.
        int nreused = 0;

        {

            for (int i = 0; i < arity; i++) {

                final IVariableOrConstant<?> t = p.get(i);

                if (t != null && t.isVar()) {

                    // this slot is a variable.

                    if (vars == null) {

                        vars = new LinkedHashMap<IVariableOrConstant<?>, Integer>();

                    }

                    Integer cnt = vars.get(t);

                    if (cnt == null) {

                        vars.put(t, cnt = Integer.valueOf(1));

                    } else {

                        final int tmp = cnt.intValue() + 1;

                        vars.put(t, cnt = Integer.valueOf(tmp));

                        // note: add 2 for the 1st dup and 1 for each
                        // additional dup thereafter.
                        noccurs += (tmp == 2 ? 2 : 1);

                        if (tmp == 2) {

                            // this variable is reused.
                            nreused++;

                        }

                    }

                }

            }

            if (nreused == 0) {

                // there are no duplicate variables.
                return null;

            }

        }

        assert vars != null;

        // allocate the array for the reused variable record(s).
        final int[] indices = new int[nreused + noccurs];

        // populate that array.
        {

            int i = 0;
            
            final Iterator<Map.Entry<IVariableOrConstant<?>, Integer>> itr = vars
                    .entrySet().iterator();

            while (itr.hasNext()) {

                final Map.Entry<IVariableOrConstant<?>, Integer> e = itr.next();
                
                final int nused = e.getValue().intValue();

                if (nused < 2)
                    continue;
                
                final IVariable<?> var = (IVariable<?>)e.getKey();
                
                indices[i++] = nused;

                for (int j = 0; j < arity; j++) {

                    final IVariableOrConstant<?> t = p.get(j);

                    if (t == var) {

                        // variable appears at this index.
                        indices[i++] = j;

                    }
                    
                }
                
            }
            
            assert i == indices.length;
            
        }
        
        return new SameVariableConstraint<E>(p, indices);

    } // factory method.

}
