/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Aug 17, 2010
 */

package com.bigdata.bop;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp.Annotations;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.btree.AbstractNode;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.relation.accesspath.IBlockingBuffer;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * Operator utility class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BOpUtility {

    private static final Logger log = Logger.getLogger(BOpUtility.class);
    
    /**
     * Pre-order recursive visitation of the operator tree (arguments only, no
     * annotations).
     */
    @SuppressWarnings("unchecked")
    public static Iterator<BOp> preOrderIterator(final BOp op) {

        return new Striterator(new SingleValueIterator(op))
                .append(preOrderIterator2(op));

    }

    /**
     * Visits the children (recursively) using pre-order traversal, but does
     * NOT visit this node.
     */
    @SuppressWarnings("unchecked")
    static private Iterator<AbstractNode> preOrderIterator2(final BOp op) {

        /*
         * Iterator visits the direct children, expanding them in turn with a
         * recursive application of the pre-order iterator.
         */

        return new Striterator(op.args().iterator()).addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            /*
             * Expand each child in turn.
             */
            protected Iterator expand(final Object childObj) {

                /*
                 * A child of this node.
                 */

                final BOp child = (BOp) childObj;

                if (child.arity() > 0) {

                    /*
                     * The child is a Node (has children).
                     * 
                     * Visit the children (recursive pre-order traversal).
                     */

                    final Striterator itr = new Striterator(
                            new SingleValueIterator(child));

                    // append this node in post-order position.
                    itr.append(preOrderIterator2(child));

                    return itr;

                } else {

                    /*
                     * The child is a leaf.
                     */

                    // Visit the leaf itself.
                    return new SingleValueIterator(child);

                }
            }
        });

    }

    /**
     * Post-order recursive visitation of the operator tree (arguments only, no
     * annotations).
     */
    @SuppressWarnings("unchecked")
    public static Iterator<BOp> postOrderIterator(final BOp op) {

        return new Striterator(postOrderIterator2(op))
                .append(new SingleValueIterator(op));

    }

    /**
     * Visits the children (recursively) using post-order traversal, but does
     * NOT visit this node.
     */
    @SuppressWarnings("unchecked")
    static private Iterator<AbstractNode> postOrderIterator2(final BOp op) {

        /*
         * Iterator visits the direct children, expanding them in turn with a
         * recursive application of the post-order iterator.
         */

        return new Striterator(op.args().iterator()).addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            /*
             * Expand each child in turn.
             */
            protected Iterator expand(final Object childObj) {

                /*
                 * A child of this node.
                 */

                final BOp child = (BOp) childObj;

                if (child.arity() > 0) {

                    /*
                     * The child is a Node (has children).
                     * 
                     * Visit the children (recursive post-order traversal).
                     */

                    final Striterator itr = new Striterator(
                            postOrderIterator2(child));

                    // append this node in post-order position.
                    itr.append(new SingleValueIterator(child));

                    return itr;

                } else {

                    /*
                     * The child is a leaf.
                     */

                    // Visit the leaf itself.
                    return new SingleValueIterator(child);

                }
            }
        });

    }

    /**
     * Visit all annotations which are {@link BOp}s (non-recursive).
     * 
     * @param op
     *            An operator.
     * 
     * @return An iterator which visits the {@link BOp} annotations in an
     *         arbitrary order.
     */
    @SuppressWarnings("unchecked")
    public static Iterator<BOp> annotationOpIterator(final BOp op) {

        return new Striterator(op.annotations().values().iterator())
                .addFilter(new Filter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean isValid(Object arg0) {
                        return arg0 instanceof BOp;
                    }
                });
        
    }

//    /**
//     * Pre-order traversal of the annotations of the operator which are
//     * themselves operators without recursion through the children of the given
//     * operator (the children of each annotation are visited, but the
//     * annotations of annotations are not).
//     * 
//     * @param op
//     *            An operator.
//     * 
//     * @return An iterator which visits the pre-order traversal or the operator
//     *         annotations.
//     */
//    @SuppressWarnings("unchecked")
//    public static Iterator<BOp> annotationOpPreOrderIterator(final BOp op) {
//        
//        // visit the node's operator annotations.
//        final Striterator itr = new Striterator(annotationOpIterator(op));
//
//        // expand each operator annotation with a pre-order traversal.
//        itr.addFilter(new Expander() {
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            protected Iterator<?> expand(final Object ann) {
//                return preOrderIterator((BOp) ann);
//            }
//        });
//
//        return (Iterator<BOp>) itr;
//        
//    }
    
    /**
     * Recursive pre-order traversal of the operator tree with visitation of all
     * operator annotations. The annotations for an operator are visited before
     * its children are visited. Only annotations whose values are {@link BOp}s
     * are visited. Annotation {@link BOp}s are also recursively visited with
     * the pre-order traversal.
     * 
     * @param op
     *            An operator.
     *            
     * @return The iterator.
     */
    @SuppressWarnings("unchecked")
    public static Iterator<BOp> preOrderIteratorWithAnnotations(final BOp op) {
       
        return new Striterator(preOrderIterator(op)).addFilter(new Expander(){

            private static final long serialVersionUID = 1L;

            @Override
            protected Iterator expand(final Object arg0) {

                final BOp op = (BOp)arg0;

                // visit the node.
                final Striterator itr = new Striterator(
                        new SingleValueIterator(op));

                // visit the node's operator annotations.
                final Striterator itr2 = new Striterator(
                        annotationOpIterator(op));

                // expand each operator annotation with a pre-order traversal.
                itr2.addFilter(new Expander() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    protected Iterator expand(final Object ann) {
                        return preOrderIteratorWithAnnotations((BOp) ann);
                    }
                });
                
                // append the pre-order traveral of each annotation.
                itr.append(itr2);

                return itr;
            }
            
        });
        
    }

    /**
     * Return all variables recursively using a pre-order traversal present
     * whether in the operator tree or on annotations attached to operators.
     */
    @SuppressWarnings("unchecked")
    public static Iterator<IVariable<?>> getSpannedVariables(final BOp op) {

        return new Striterator(preOrderIteratorWithAnnotations(op))
                .addFilter(new Filter() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean isValid(Object arg0) {
                        return arg0 instanceof IVariable<?>;
                    }
                });

    }

    /**
     * Return the variables from the operator's arguments.
     * 
     * @param op
     *            The operator.
     *            
     * @return An iterator visiting its {@link IVariable} arguments.
     */
    @SuppressWarnings("unchecked")
    static public Iterator<IVariable<?>> getArgumentVariables(final BOp op) {

        return new Striterator(op.args().iterator())
                .addFilter(new Filter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean isValid(final Object arg0) {
                        return arg0 instanceof IVariable<?>;
                    }
                });

    }

    /**
     * The #of arguments to this operation which are variables. This method does
     * not report on variables in child nodes nor on variables in attached
     * {@link IConstraint}, etc.
     */
    static public int getArgumentVariableCount(final BOp op) {
        int nvars = 0;
        final Iterator<BOp> itr = op.args().iterator();
        while(itr.hasNext()) {
            final BOp arg = itr.next();
            if (arg instanceof IVariable<?>)
                nvars++;
        }
        return nvars;
    }

    /**
     * Return an index from the {@link BOp.Annotations#BOP_ID} to the
     * {@link BOp} for each spanned {@link BOp} (including annotations).
     * {@link BOp}s without identifiers are not indexed.
     * <p>
     * {@link BOp}s should form directed acyclic graphs, but this is not
     * strictly enforced. The recursive traversal iterators declared by this
     * class do not protect against loops in the operator tree. However,
     * {@link #getIndex(BOp)} detects and report loops based on duplicate
     * {@link Annotations#BOP_ID}s -or- duplicate {@link BOp} references.
     * 
     * @param op
     *            A {@link BOp}.
     * 
     * @return The index, which is immutable and thread-safe.
     * 
     * @throws DuplicateBOpIdException
     *             if there are two or more {@link BOp}s having the same
     *             {@link Annotations#BOP_ID}.
     * @throws BadBOpIdTypeException
     *             if the {@link Annotations#BOP_ID} is not an {@link Integer}.
     * @throws DuplicateBOpException
     *             if the same {@link BOp} appears more once in the operator
     *             tree and it is neither an {@link IVariable} nor an
     *             {@link IConstant}.
     */
    static public Map<Integer,BOp> getIndex(final BOp op) {
        final LinkedHashMap<Integer, BOp> map = new LinkedHashMap<Integer, BOp>();
        final LinkedHashSet<BOp> distinct = new LinkedHashSet<BOp>();
        final Iterator<BOp> itr = preOrderIteratorWithAnnotations(op);
        while (itr.hasNext()) {
            final BOp t = itr.next();
            final Object x = t.getProperty(Annotations.BOP_ID);
            if (x != null) {
                if (!(x instanceof Integer)) {
                    throw new BadBOpIdTypeException("Must be Integer, not: "
                            + x.getClass() + ": " + Annotations.BOP_ID);
                }
                final Integer id = (Integer) t.getProperty(Annotations.BOP_ID);
                final BOp conflict = map.put(id, t);
                if (conflict != null)
                    throw new DuplicateBOpIdException("duplicate id=" + id
                            + " for " + conflict + " and " + t);
            }
            if (!distinct.add(t) && !(t instanceof IVariableOrConstant<?>)) {
                /*
                 * BOp appears more than once. This is only allowed for
                 * constants and variables to reduce the likelihood of operator
                 * trees which describe loops. This will not detect operator
                 * trees whose sinks target a descendant, which is another way
                 * to create a loop.
                 */
                throw new DuplicateBOpException("dup=" + t + ", root="
                        + toString(op));
            }
        }
        // wrap to ensure immutable and thread-safe.
        return Collections.unmodifiableMap(map);
    }

    /**
     * Return the parent of the operator in the operator tree (this does not
     * search the annotations, just the children).
     * <p>
     * Note that {@link Var} is a singleton pattern for each distinct variable
     * node, so there can be multiple parents for a {@link Var}.
     * 
     * @param root
     *            The root of the operator tree (or at least a known ancestor of
     *            the operator).
     * @param op
     *            The operator.
     * 
     * @return The parent -or- <code>null</code> if <i>op</i> is not found in
     *         the operator tree.
     * 
     * @throws IllegalArgumentException
     *             if either argument is <code>null</code>.
     */
    static public BOp getParent(final BOp root, final BOp op) {

        if (root == null)
            throw new IllegalArgumentException();

        if (op == null)
            throw new IllegalArgumentException();

        final Iterator<BOp> itr = root.args().iterator();

        while (itr.hasNext()) {

            final BOp current = itr.next();

            if (current == op)
                return root;

            final BOp found = getParent(current, op);

            if (found != null)
                return found;
            
        }

        return null;

    }

    /**
     * Return the left-deep child of the operator, halting at a leaf or earlier
     * if a control operator is found.
     * 
     * @param op
     *            The operator.
     * 
     * @return The child where pipeline evaluation should begin.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * 
     * @todo This does not protect against loops in the operator tree.
     * 
     * @todo unit tests.
     */
    static public BOp getPipelineStart(BOp op) {

        if (op == null)
            throw new IllegalArgumentException();

        while (true) {
            if (op.getProperty(BOp.Annotations.CONTROLLER,
                    BOp.Annotations.DEFAULT_CONTROLLER)) {
                // Halt at a control operator.
                return op;
            }
            if(op.arity()==0) {
                // No children.
                return op;
            }
            final BOp left = op.get(0);
            if (left == null) {
                // Halt at a leaf.
                return op;
            }
            // Descend through the left child.
            op = left;
        }

    }

    /**
     * Return the effective default sink.
     * 
     * @param bop
     *            The operator.
     * @param p
     *            The parent of that operator, if any.
     * 
     * @todo unit tests.
     */
    static public Integer getEffectiveDefaultSink(final BOp bop, final BOp p) {

        if (bop == null)
            throw new IllegalArgumentException();

        Integer sink;

        // Explicitly specified sink?
        sink = (Integer) bop.getProperty(PipelineOp.Annotations.SINK_REF);

        if (sink == null) {
            if (p == null) {
                // No parent, so no sink.
                return null;
            }
            // The parent is the sink.
            sink = (Integer) p
                    .getRequiredProperty(BOp.Annotations.BOP_ID);
        }

        return sink;

    }

	/**
	 * Return a list containing the evaluation order for the pipeline. Only the
	 * child operands are visited. Operators in subqueries are not visited since
	 * they will be assigned {@link BOpStats} objects when they are run as a
	 * subquery. The evaluation order is given by the depth-first left-deep
	 * traversal of the query.
	 * 
	 * @todo unit tests.
	 */
    public static Integer[] getEvaluationOrder(final BOp op) {

    	final List<Integer> order = new LinkedList<Integer>();
    	
    	getEvaluationOrder(op, order, 0/*depth*/);
    	
    	return order.toArray(new Integer[order.size()]);
    	
    }
    
    private static void getEvaluationOrder(final BOp op, final List<Integer> order, final int depth) {
    	
        if(!(op instanceof PipelineOp))
            return;
        
        final int bopId = op.getId();

		if (depth == 0
				|| !op.getProperty(BOp.Annotations.CONTROLLER,
						BOp.Annotations.DEFAULT_CONTROLLER)) {

			if (op.arity() > 0) {

				// left-deep recursion
				getEvaluationOrder(op.get(0), order, depth + 1);

			}

		}

        order.add(bopId);

    }
    
    /**
     * Combine chunks drawn from an iterator into a single chunk. This is useful
     * when materializing intermediate results for an all-at-once operator.
     * 
     * @param itr
     *            The iterator
     * @param stats
     *            {@link BOpStats#chunksIn} and {@link BOpStats#unitsIn} are
     *            updated.
     * 
     * @return A single chunk containing all of the chunks visited by the
     *         iterator.
     * 
     * @todo unit tests.
     */
    static public IBindingSet[] toArray(final Iterator<IBindingSet[]> itr,
            final BOpStats stats) {

        final List<IBindingSet[]> list = new LinkedList<IBindingSet[]>();

        int nchunks = 0, nelements = 0;
        {

            while (itr.hasNext()) {

                final IBindingSet[] a = itr.next();

                list.add(a);

                nchunks++;

                nelements += a.length;

                list.add(a);

            }

            stats.chunksIn.add(nchunks);
            stats.unitsIn.add(nelements);

        }

        if (nchunks == 0) {

            return new IBindingSet[0];
            
        } else if (nchunks == 1) {
            
            return list.get(0);
            
        } else {
            
            int n = 0;
            
            final IBindingSet[] a = new IBindingSet[nelements];
            
            final Iterator<IBindingSet[]> itr2 = list.iterator();
            
            while (itr2.hasNext()) {
            
                final IBindingSet[] t = itr2.next();
                
                System.arraycopy(t/* src */, 0/* srcPos */, a/* dest */,
                        n/* destPos */, t.length/* length */);
                
                n += t.length;
            
            }

            return a;

        }

    } // toArray()

    /**
     * Pretty print a bop.
     * 
     * @param bop
     *            The bop.
     * 
     * @return The formatted representation.
     */
    public static String toString(final BOp bop) {

        final StringBuilder sb = new StringBuilder();

        toString(bop, sb, 0);

        // chop off the last \n
        sb.setLength(sb.length() - 1);

        return sb.toString();

    }

    private static void toString(final BOp bop, final StringBuilder sb,
            final int indent) {

        sb.append(indent(indent)).append(
                bop == null ? "<null>" : bop.toString()).append('\n');

        if (bop == null)
            return;
        
        for (BOp arg : bop.args()) {

            if (arg.arity() > 0) {
             
                toString(arg, sb, indent+1);
                
            }

        }

    }

    /**
     * Returns a string that may be used to indent a dump of the nodes in
     * the tree.
     * 
     * @param height
     *            The height.
     *            
     * @return A string suitable for indent at that height.
     */
    private static String indent(final int height) {

        if( height == -1 ) {
        
            // The height is not defined.
            
            return "";
            
        }
        
        return ws.substring(0, height * 4);

    }

    private static final transient String ws = "                                                                                                                                                                                                                  ";

//    /**
//     * Verify that all bops from the identified <i>startId</i> to the root are
//     * {@link PipelineOp}s and have an assigned {@link BOp.Annotations#BOP_ID}.
//     * This is required in order for us to be able to target messages to those
//     * operators.
//     * 
//     * @param startId
//     *            The {@link BOp.Annotations#BOP_ID} at which the query will
//     *            start. This is typically the left-most descendant.
//     * @param root
//     *            The root of the operator tree.
//     * 
//     * @throws RuntimeException
//     *             if the operator tree does not meet any of the criteria for
//     *             pipeline evaluation.
//     */
//    public static void verifyPipline(final int startId, final BOp root) {
//
//        throw new UnsupportedOperationException();
//
//    }

    /**
     * Check constraints.
     * 
     * @param constraints
     * @param bindingSet
     * 
     * @return <code>true</code> iff the constraints are satisfied.
     */
    static public boolean isConsistent(final IConstraint[] constraints,
            final IBindingSet bindingSet) {

        for (int i = 0; i < constraints.length; i++) {

            final IConstraint constraint = constraints[i];

            if (!constraint.accept(bindingSet)) {

                if (log.isDebugEnabled()) {

                    log.debug("Rejected by "
                            + constraint.getClass().getSimpleName() + " : "
                            + bindingSet);

                }

                return false;

            }

            if (log.isTraceEnabled()) {

                log.debug("Accepted by "
                        + constraint.getClass().getSimpleName() + " : "
                        + bindingSet);

            }

        }

        return true;

    }
    
    /**
     * Copy binding sets from the source to the sink(s).
     * 
     * @param source
     *            The source.
     * @param sink
     *            The sink (required).
     * @param sink2
     *            Another sink (optional).
     * @param constraints
     *            Binding sets which fail these constraints will NOT be copied
     *            (optional).
     * @param stats
     *            The {@link BOpStats#chunksIn} and {@link BOpStats#unitsIn}
     *            will be updated during the copy (optional).
     */
    static public void copy(
            final IAsynchronousIterator<IBindingSet[]> source,
            final IBlockingBuffer<IBindingSet[]> sink,
            final IBlockingBuffer<IBindingSet[]> sink2,
            final IConstraint[] constraints, final BOpStats stats) {

        while (source.hasNext()) {

            final IBindingSet[] chunk = source.next();

            if (stats != null) {

                stats.chunksIn.increment();

                stats.unitsIn.add(chunk.length);

            }

            // apply optional constraints.
            final IBindingSet[] tmp = applyConstraints(chunk,constraints);
            
//            System.err.println("Copying: "+Arrays.toString(tmp));
            
            // copy accepted binding sets to the default sink.
            sink.add(tmp);
            
            if (sink2 != null) {
                // copy accepted binding sets to the alt sink.
                sink2.add(tmp);
            }
            
        }
        
    }

    /**
     * Return a dense array containing only those {@link IBindingSet}s which
     * satisfy the constraints.
     * 
     * @param chunk
     *            A chunk of binding sets.
     * @param constraints
     *            The constraints (optional).
     *            
     * @return The dense chunk of binding sets.
     */
    static private IBindingSet[] applyConstraints(final IBindingSet[] chunk,
            final IConstraint[] constraints) {

        if (constraints == null) {

            /*
             * No constraints, copy all binding sets.
             */

            return chunk;

        }

        /*
         * Copy binding sets which satisfy the constraint(s).
         */

        IBindingSet[] t = new IBindingSet[chunk.length];

        int j = 0;

        for (int i = 0; i < chunk.length; i++) {

            final IBindingSet bindingSet = chunk[i];

            if (BOpUtility.isConsistent(constraints, bindingSet)) {

                t[j++] = bindingSet;

            }

        }

        if (j != chunk.length) {

            // allocate exact size array.
            final IBindingSet[] tmp = (IBindingSet[]) java.lang.reflect.Array
                    .newInstance(chunk[0].getClass(), j);

            // make a dense copy.
            System.arraycopy(t/* src */, 0/* srcPos */, tmp/* dst */,
                    0/* dstPos */, j/* len */);

            t = tmp;

        }

        return t;

    }

	/**
	 * Inject (or replace) an {@link Integer} "rowId" column. This does not have
	 * a side-effect on the source {@link IBindingSet}s.
	 * 
	 * @param var
	 *            The name of the column.
	 * @param start
	 *            The starting value for the identifier.
	 * @param in
	 *            The source {@link IBindingSet}s.
	 * 
	 * @return The modified {@link IBindingSet}s.
	 */
	public static IBindingSet[] injectRowIdColumn(final IVariable var,
			final int start, final IBindingSet[] in) {

		if (in == null)
			throw new IllegalArgumentException();
		
		final IBindingSet[] out = new IBindingSet[in.length];
		
		for (int i = 0; i < out.length; i++) {
		
			final IBindingSet bset = in[i].clone();

			bset.set(var, new Constant<Integer>(Integer.valueOf(start + i)));

			out[i] = bset;
			
		}
		
		return out;

	}

}
