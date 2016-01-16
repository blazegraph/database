/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp.Annotations;
import com.bigdata.bop.aggregate.IAggregate;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.bop.solutions.GroupByOp;
import com.bigdata.bop.solutions.GroupByRewriter;
import com.bigdata.bop.solutions.IGroupByRewriteState;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.striterator.CloseableIteratorWrapper;

import cutthecrap.utils.striterators.EmptyIterator;
import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.ICloseable;
import cutthecrap.utils.striterators.ICloseableIterator;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * Operator utility class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BOpUtility {

    private static transient final Logger log = Logger
            .getLogger(BOpUtility.class);
    
    /**
     * Pre-order recursive visitation of the operator tree (arguments only, no
     * annotations).
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Iterator<BOp> preOrderIterator(final BOp op) {

        return new Striterator(new SingleValueIterator(op))
                .append(preOrderIterator2(0,op));

    }

    /**
     * Visits the children (recursively) using pre-order traversal, but does NOT
     * visit this node.
     * 
     * @param stack
     */
    @SuppressWarnings("unchecked")
    static private Iterator<BOp> preOrderIterator2(final int depth, final BOp op) {

        /*
         * Iterator visits the direct children, expanding them in turn with a
         * recursive application of the pre-order iterator.
         */
    	
		// mild optimization when no children are present.
		if (op == null || op.arity() == 0)
			return EmptyIterator.DEFAULT;

        return new Striterator(op.argIterator()).addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            /*
             * Expand each child in turn.
             */
            protected Iterator expand(final Object childObj) {

                /*
                 * A child of this node.
                 */

                final BOp child = (BOp) childObj;

                /*
                 * TODO The null child reference which can occur here is the [c]
                 * of the StatementPatternNode. We might want to make [c] an
                 * anonymous variable instead of having a [null].
                 */
                if (child != null && child.arity() > 0) {

                    /*
                     * The child is a Node (has children).
                     * 
                     * Visit the children (recursive pre-order traversal).
                     */

                    // append this node in pre-order position.
                    final Striterator itr = new Striterator(
                            new SingleValueIterator(child));

                    // append children
                    itr.append(preOrderIterator2(depth + 1, child));

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
     * 
     * @param context
     */
    @SuppressWarnings("unchecked")
    static private Iterator<BOp> postOrderIterator2(final BOp op) {

        /*
         * Iterator visits the direct children, expanding them in turn with a
         * recursive application of the post-order iterator.
         */

        if (op == null || op.arity() == 0)
            return EmptyIterator.DEFAULT;

        return new Striterator(op.argIterator()).addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            /*
             * Expand each child in turn.
             */
            protected Iterator expand(final Object childObj) {

                /*
                 * A child of this node.
                 */

                final BOp child = (BOp) childObj;

                /*
                 * TODO The null child reference which can occur here is the [c]
                 * of the StatementPatternNode. We might want to make [c] an
                 * anonymous variable instead of having a [null].
                 */
                if (child != null && child.arity() > 0) {

                    /*
                     * The child is a Node (has children).
                     * 
                     * Visit the children (recursive post-order traversal).
                     */

                    // visit children.
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

        return new Striterator(preOrderIterator(op)).addFilter(new Expander() {

            private static final long serialVersionUID = 1L;

            @Override
            protected Iterator expand(final Object arg0) {

                final BOp op = (BOp)arg0;

                // visit the node.
                final Striterator itr = new Striterator(
                        new SingleValueIterator(op));

                /*
                 * FIXME In order to visit the annotations as NV pairs
                 * we need to modify this part of the expander pattern,
                 * perhaps just by pushing the appropriate NV object
                 * onto the stack? Or maybe just push the attribute name
                 * rather than the attribute name and value? Do this
                 * for both of the XXXOrderIteratorwithAnnotation() methods.
                 */
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
                
                // append the pre-order traversal of each annotation.
                itr.append(itr2);

                return itr;
            }
            
        });
        
    }

    /**
     * Recursive post-order traversal of the operator tree with visitation of
     * all operator annotations. The annotations for an operator are visited
     * before its children are visited. Only annotations whose values are
     * {@link BOp}s are visited. Annotation {@link BOp}s are also recursively
     * visited with the pre-order traversal.
     * 
     * @param op
     *            An operator.
     * 
     * @return The iterator.
     * 
    * @see <a href="http://trac.bigdata.com/ticket/1210" >
    *      BOpUtility.postOrderIteratorWithAnnotations() is has wrong visitation
    *      order. </a>
     */
    @SuppressWarnings("unchecked")
    public static Iterator<BOp> postOrderIteratorWithAnnotations(final BOp op) {
       
        // visit the node's operator annotations.
        final Striterator itr = new Striterator(
                annotationOpIterator(op));

        itr.append(op.argIterator());
        
        // expand each operator annotation with a post-order traversal.
        itr.addFilter(new Expander() {
            private static final long serialVersionUID = 1L;

            @Override
            protected Iterator<BOp> expand(final Object ann) {
                return postOrderIteratorWithAnnotations((BOp) ann);
            }
            
        });
        
        // visit the node.
        itr.append(new SingleValueIterator<BOp>(op));

        
        return itr;
        
//        return new Striterator(postOrderIterator(op)).addFilter(new Expander(){
//
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            protected Iterator<BOp> expand(final Object arg0) {
//
//                final BOp op = (BOp)arg0;
//
//                // visit the node's operator annotations.
//                final Striterator itr = new Striterator(
//                        annotationOpIterator(op));
//
//                // expand each operator annotation with a post-order traversal.
//                itr.addFilter(new Expander() {
//                    private static final long serialVersionUID = 1L;
//
//                    @Override
//                    protected Iterator<BOp> expand(final Object ann) {
//                        return postOrderIteratorWithAnnotations((BOp) ann);
//                    }
//                    
//                });
//                
//                // visit the node.
//                itr.append(new SingleValueIterator<BOp>(op));
//
//                return itr;
//            }
//            
//        });
        
    }

    /**
     * Return the distinct variables recursively using a pre-order traversal
     * present whether in the operator tree or on annotations attached to
     * operators.
     * <p>
     * Note: This will find variables within subqueries as well, which may not
     * be what is intended.
     * 
     * @see StaticAnalysis#getSpannedVariables(BOp, Set), which will only report
     *      the variables which are actually visible (variables not projected
     *      from subqueries are not reported).
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
                }).makeUnique();

    }

    /**
     * Return an {@link List} containing the {@link IVariable}s visited by the
     * iterator.
     * 
     * @param it
     *            The iterator.
     */
//    @SuppressWarnings("rawtypes")
//    public static List<IVariable> toList(final Iterator<IVariable<?>> it) {
//
//        final List<IVariable> c = new LinkedList<IVariable>();
//
//        while (it.hasNext()) {
//
//            final IVariable v = it.next();
//
//            c.add(v);
//
//        }
//
//        return c;
//        
//    }
    public static <T> List<T> toList(final Iterator<T> it) {

        final List<T> c = new LinkedList<T>();

        while (it.hasNext()) {

            final T v = it.next();

            c.add(v);

        }

        return c;
        
    }
    
    /**
     * Return an array containing the {@link IVariable}s visited by the
     * iterator.
     * 
     * @param it
     *            The iterator.
     */
    @SuppressWarnings("rawtypes")
    public static IVariable[] toArray(final Iterator<IVariable<?>> it) {

        return toList(it).toArray(new IVariable[] {});

    }

    /**
     * Return a list containing references to all nodes of the given type
     * (recursive, including annotations).
     * <p>
     * Note: This may be used to work around concurrent modification errors
     * since the returned list is structurally independent of the original
     * operator tree.
     * 
     * @param op
     *            The root of the operator tree.
     * @param clas
     *            The type of the node to be extracted.
     * 
     * @return A list containing those references.
     * 
     * @see #visitAll(BOp, Class)
     */
    public static <C> List<C> toList(final BOp op, final Class<C> clas) {
        
        final List<C> list = new LinkedList<C>();
        
        final Iterator<C> it = visitAll(op,clas);
        
        while(it.hasNext()) {

            list.add(it.next());
            
        }
        
        return list;
        
    }

    /**
     * Return the sole instance of the specified class.
     * 
     * @param op
     *            The root of the traversal.
     * @param class1
     *            The class to look for.
     * @return The sole instance of that class.
     * @throws NoSuchElementException
     *             if there is no such instance.
     * @throws RuntimeException
     *             if there is more than one such instance.
     */
    public static <C> C getOnly(final BOp op, final Class<C> class1) {
        final Iterator<C> it = visitAll(op, class1);
        if (!it.hasNext())
            throw new NoSuchElementException("No instance found: class="
                    + class1);
        final C ret = it.next();
        if (it.hasNext())
            throw new RuntimeException("More than one instance exists: class="
                    + class1);
        return ret;
    }

    /**
     * Return an iterator visiting references to all nodes of the given type
     * (recursive, including annotations).
     * 
     * @param op
     *            The root of the operator tree.
     * @param clas
     *            The type of the node to be extracted.
     * 
     * @return A iterator visiting those references.
     * 
     * @see #toList(BOp, Class)
     */
    @SuppressWarnings("unchecked")
	public static <C> Iterator<C> visitAll(final BOp op, final Class<C> clas) {
    	
        return new Striterator(preOrderIteratorWithAnnotations(op))
		        .addFilter(new Filter() {
		            private static final long serialVersionUID = 1L;
		
		            @Override
		            public boolean isValid(Object arg0) {
		                return clas.isAssignableFrom(arg0.getClass());
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

        return new Striterator(op.argIterator())
                .addFilter(new Filter() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public boolean isValid(final Object arg0) {
                        return arg0 instanceof IVariable<?>;
                    }
                });

    }

//    /**
//     * Return the variables from the operator's arguments.
//     * 
//     * @param op
//     *            The operator.
//     * 
//     * @return An iterator visiting its {@link IVariable} arguments.
//     * 
//     *         TODO This gets used a LOT, but ONLY by
//     *         {@link AccessPath#solutions(BaseJoinStats). As written, it uses a
//     *         hash map to decide which are the DISTINCT variables in the
//     *         args[]. However variables support reference testing. The fastest
//     *         way to write this method is on the BOP classes. They can spin
//     *         through their args[] and count the distinct variables and then
//     *         copy them into a new array.
//     */
//    @SuppressWarnings("unchecked")
//    static public IVariable<?>[] getDistinctArgumentVariables(final BOp op) {
//        
//        return BOpUtility.toArray(
//        new Striterator(op.argIterator())
//                .addFilter(new Filter() {
//
//                    private static final long serialVersionUID = 1L;
//
//                    @Override
//                    public boolean isValid(final Object arg0) {
//                        return arg0 instanceof IVariable<?>;
//                    }
//                }).makeUnique()
//                );
//
//    }

    /**
     * The #of arguments to this operation which are variables. This method does
     * not report on variables in child nodes nor on variables in attached
     * {@link IConstraint}, etc.
     */
    static public int getArgumentVariableCount(final BOp op) {
        int nvars = 0;
        final Iterator<BOp> itr = op.argIterator();
        while(itr.hasNext()) {
            final BOp arg = itr.next();
            if (arg instanceof IVariable<?>)
                nvars++;
        }
        return nvars;
    }

    /**
     * Return an index from the {@link BOp.Annotations#BOP_ID} to the
     * {@link BOp} for each spanned {@link PipelineOp}. {@link BOp}s without
     * identifiers are not indexed. It is an error a non-{@link PipelineOp} is
     * encountered.
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
     * @throws NoBOpIdException
     *             if a {@link PipelineOp} does not have a
     *             {@link Annotations#BOP_ID}.
     * @throws NotPipelineOpException
     *             if <i>op</i> or any of its arguments visited during recursive
     *             traversal are not a {@link PipelineOp}.
     */
    static public Map<Integer,BOp> getIndex(final BOp op) {
        if(op == null)
            throw new IllegalArgumentException();
        final LinkedHashMap<Integer, BOp> map = new LinkedHashMap<Integer, BOp>();
//        final LinkedHashSet<BOp> distinct = new LinkedHashSet<BOp>();
        final Iterator<BOp> itr = preOrderIterator(op);//WithAnnotations(op);
        while (itr.hasNext()) {
            final BOp t = itr.next();
            if(!(t instanceof PipelineOp))
                throw new NotPipelineOpException(t.toString());
            final Object x = t.getProperty(Annotations.BOP_ID);
            if (x == null) {
                throw new NoBOpIdException(t.toString());
            }
            if (!(x instanceof Integer)) {
                throw new BadBOpIdTypeException("Must be Integer, not: "
                        + x.getClass() + ": " + Annotations.BOP_ID);
            }
            final Integer id = (Integer) t.getProperty(Annotations.BOP_ID);
            final BOp conflict = map.put(id, t);
            if (conflict != null) {
                /*
                 * BOp appears more than once. This is not allowed for
                 * pipeline operators. If you are getting this exception for
                 * a non-pipeline operator, you should remove the bopId.
                 */
                throw new DuplicateBOpIdException("duplicate id=" + id
                        + " for " + conflict + " and " + t);
            }
//                if (op instanceof PipelineOp && !distinct.add(op)) {
//                    /*
//                     * BOp appears more than once. This is not allowed for
//                     * pipeline operators. If you are getting this exception for
//                     * a non-pipeline operator, you should remove the bopId.
//                     */
//                    throw new DuplicateBOpException("dup=" + t + ", root="
//                            + toString(op));
//                }
//            if (!distinct.add(t) && !(t instanceof IValueExpression<?>)
//                    && !(t instanceof Constraint)) {
//                /*
//                 * BOp appears more than once. This is only allowed for
//                 * constants and variables to reduce the likelihood of operator
//                 * trees which describe loops. This will not detect operator
//                 * trees whose sinks target a descendant, which is another way
//                 * to create a loop.
//                 */
//                throw new DuplicateBOpException("dup=" + t + ", root="
//                        + toString(op));
//            }
        }
        // wrap to ensure immutable and thread-safe.
        return Collections.unmodifiableMap(map);
    }

//    /**
//     * Lookup the first operator in the specified conditional binding group and
//     * return its bopId.
//     * 
//     * @param query
//     *            The query plan.
//     * @param groupId
//     *            The identifier for the desired conditional binding group.
//     * 
//     * @return The bopId of the first operator in that conditional binding group
//     *         -or- <code>null</code> if the specified conditional binding group
//     *         does not exist in the query plan.
//     *         
//     * @throws IllegalArgumentException
//     *             if either argument is <code>null</code>.
//     * 
//     * @see PipelineOp.Annotations#CONDITIONAL_GROUP
//     * @see PipelineOp.Annotations#ALT_SINK_GROUP
//     */
//    static public Integer getFirstBOpIdForConditionalGroup(final BOp query,
//            final Integer groupId) {
//        if (query == null)
//            throw new IllegalArgumentException();
//        if (groupId == null)
//            throw new IllegalArgumentException();
//        final Iterator<BOp> itr = postOrderIterator(query);
//        while (itr.hasNext()) {
//            final BOp t = itr.next();
//            final Object x = t.getProperty(PipelineOp.Annotations.CONDITIONAL_GROUP);
//            if (x != null) {
//                if (!(x instanceof Integer)) {
//                    throw new BadConditionalGroupIdTypeException(
//                            "Must be Integer, not: " + x.getClass() + ": "
//                                    + PipelineOp.Annotations.CONDITIONAL_GROUP);
//                }
//                final Integer id = (Integer) t
//                        .getProperty(PipelineOp.Annotations.CONDITIONAL_GROUP);
//                if(id.equals(groupId)) {
//                    /*
//                     * Return the BOpId associated with the first operator in
//                     * the pre-order traversal of the query plan which has the
//                     * specified groupId.
//                     */
//                    return t.getId();
//                }
//            }
//        }
//        // No such groupId in the query plan.
//        return null;
//    }
    
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

        final int arity = root.arity();
        
        for (int i = 0; i < arity; i++) {
//        final Iterator<BOp> itr = root.argIterator();
//
//        while (itr.hasNext()) {
//
//            final BOp current = itr.next();
            
            final BOp current = root.get(i);
            
            if (current == op)
                return root;

            final BOp found = getParent(current, op);

            if (found != null)
                return found;
            
        }

        return null;

    }

    /**
     * Return the left-deep child of the operator.
     * <p>
     * Note: This does not protect against loops in the operator tree.
     * 
     * @param op
     *            The operator.
     * 
     * @return The child where pipeline evaluation should begin.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     */
    static public BOp getPipelineStart(BOp op) {

        if (op == null)
            throw new IllegalArgumentException();

        while (true) {
//            if (op.getProperty(BOp.Annotations.CONTROLLER,
//                    BOp.Annotations.DEFAULT_CONTROLLER)) {
//                // Halt at a control operator.
//                return op;
//            }
            if (op.arity() == 0) {
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

        if (!itr.hasNext()) {

            /*
             * Fast path for an empty chunk.
             */

            return EMPTY_CHUNK;

        }

        final IBindingSet[] firstChunk = itr.next();

        if (!itr.hasNext()) {

            /*
             * Fast path if everything is already in one chunk.
             */

            if (stats != null) {
                stats.chunksIn.add(1);
                stats.unitsIn.add(firstChunk.length);
            }

            return firstChunk;

        }

        final List<IBindingSet[]> list = new LinkedList<IBindingSet[]>();

        list.add(firstChunk);

        int nchunks = 1, nelements = firstChunk.length;

        try {

            while (itr.hasNext()) {

                final IBindingSet[] a = itr.next();

                list.add(a);

                nchunks++;

                nelements += a.length;

            }

            if (stats != null) {
                stats.chunksIn.add(nchunks);
                stats.unitsIn.add(nelements);
            }

        } finally {
            
            if (itr instanceof ICloseable) {
             
                ((ICloseable) itr).close();
                
            }
            
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
				try {
					System.arraycopy(t/* src */, 0/* srcPos */, a/* dest */,
							n/* destPos */, t.length/* length */);
				} catch (IndexOutOfBoundsException ex) {
					// Provide some more detail in the stack trace.
					final IndexOutOfBoundsException ex2 = new IndexOutOfBoundsException(
							"t.length=" + t.length + ", a.length=" + a.length
									+ ", n=" + n);
					ex2.initCause(ex);
					throw ex2;
				}

				n += t.length;

			}

            return a;

        }

    } // toArray()

    /**
     * An empty {@link IBindingSet}[].
     */
    public static final IBindingSet[] EMPTY_CHUNK = new IBindingSet[0];

    /**
	 * Wrap the solutions with an {@link ICloseableIterator}.
	 * 
	 * @param bindingSets
	 *            The solutions.
	 *            
	 * @return The {@link ICloseableIterator}.
	 */
	public static ICloseableIterator<IBindingSet[]> asIterator(
			final IBindingSet[] bindingSets) {

		return new CloseableIteratorWrapper<IBindingSet[]>(
				new SingleValueIterator<IBindingSet[]>(bindingSets));

	}
    
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
    
    public static String toString2(final BOp bop) {

        String s = toString(bop);
        s = s.replaceAll("com.bigdata.bop.controller.", "");
        s = s.replaceAll("com.bigdata.bop.join.", "");
        s = s.replaceAll("com.bigdata.bop.solutions.", "");
        s = s.replaceAll("com.bigdata.bop.rdf.filter.", "");
        s = s.replaceAll("com.bigdata.bop.bset", "");
        s = s.replaceAll("com.bigdata.bop.", "");
        s = s.replaceAll("com.bigdata.rdf.sail.", "");
        s = s.replaceAll("com.bigdata.rdf.spo.", "");
        s = s.replaceAll("com.bigdata.rdf.internal.constraints.", "");
        return s;
        
    }

    private static void toString(final BOp bop, final StringBuilder sb,
            final int indent) {

        sb.append(indent(indent)).append(
                bop == null ? "<null>" : bop.toString()).append('\n');

        if (bop == null)
            return;
        
        /*
         * Render annotations which are bops as well. For example, this shows us
         * the subquery plans.
         */
        for(Map.Entry<String, Object> e : bop.annotations().entrySet()) {
            
            if (e.getValue() instanceof PipelineOp) {

                sb.append(indent(indent)).append("@").append(e.getKey())
                        .append(":\n");

                toString((BOp) e.getValue(), sb, indent + 1);
                
            }
            
        }

        /*
         * Render the child bops.
         */
    	final Iterator<BOp> itr = bop.argIterator();

    	while(itr.hasNext()) {
        
        	final BOp arg = itr.next();
        
            if (!(arg instanceof IVariableOrConstant<?>)) {
             
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

        return CoreBaseBOp.indent(height);
        
    }

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
     * <p>
     * Note: You MUST use {@link IBlockingBuffer#flush()} to flush the sink(s)
     * in order for the last chunk in the sink(s) to be pushed to the downstream
     * operator for that sink. However, do NOT use flush() if the operator
     * evaluation fails since you do not want to push outputs from a failed
     * operator to downstream operators.
     * 
     * @param source
     *            The source.
     * @param sink
     *            The sink (required).
     * @param sink2
     *            Another sink (optional).
     * @param mergeSolution
     *            A solution to be merged in with each solution copied from the
     *            <i>source</i> (optional). When given, the <i>selectVars</i>
     *            will be applied first to prune the copied solutions to just
     *            the selected variables and any additional bindings will then
     *            be added from the <i>mergeSolution</i>. This supports the
     *            lexical scoping on variables within a subquery when that
     *            subquery is being pipelined.
     * @param selectVars
     *            The variables to be retained (optional). When not specified,
     *            all variables will be retained.
     * @param constraints
     *            Binding sets which fail these constraints will NOT be copied
     *            (optional).
     * @param stats
     *            The {@link BOpStats#chunksIn} and {@link BOpStats#unitsIn}
     *            will be updated during the copy (optional).
     * 
     * @return The #of binding sets copied.
     */
    static public long copy(
            final Iterator<IBindingSet[]> source,
            final IBlockingBuffer<IBindingSet[]> sink,
            final IBlockingBuffer<IBindingSet[]> sink2,
            final IBindingSet mergeSolution,
            final IVariable<?>[] selectVars,//
            final IConstraint[] constraints, //
            final BOpStats stats//
            ) {

    	long nout = 0;
    	
        while (source.hasNext()) {

            final IBindingSet[] chunk = source.next();

            if (stats != null) {

                stats.chunksIn.increment();

                stats.unitsIn.add(chunk.length);

            }

            // apply optional constraints and optional SELECT.
            final IBindingSet[] tmp = applyConstraints(chunk, mergeSolution,
                    selectVars, constraints);

            // copy accepted binding sets to the default sink.
            sink.add(tmp);
            
            nout += tmp.length;
            
            if (sink2 != null) {

            	// copy accepted binding sets to the alt sink.
                sink2.add(tmp);
                
            }
            
        }
        
        return nout;
        
    }

    /**
     * Return a dense array containing only those {@link IBindingSet}s which
     * satisfy the constraints.
     * 
     * @param chunk
     *            A chunk of binding sets.
     * @param mergeSolution
     *            A solution to be merged in with each solution copied from the
     *            <i>source</i> (optional). When given, the <i>selectVars</i>
     *            will be applied first to prune the copied solutions to just
     *            the selected variables and any additional bindings will then
     *            be added from the <i>mergeSolution</i>. This supports the
     *            lexical scoping on variables within a subquery when that
     *            subquery is being pipelined.
     * @param selectVars
     *            The variables to be retained (optional). When not specified,
     *            all variables will be retained.
     * @param constraints
     *            The constraints (optional).
     *            
     * @return The dense chunk of binding sets.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    static private IBindingSet[] applyConstraints(//
            final IBindingSet[] chunk,//
            final IBindingSet mergeSolution,//
            final IVariable<?>[] selectVars,//
            final IConstraint[] constraints//
            ) {

        if (constraints == null && selectVars == null && mergeSolution == null) {

            /*
             * No constraints and everything is selected, so just return the
             * caller's chunk.
             */

            return chunk;

        }

        /*
         * Copy binding sets which satisfy the constraint(s).
         */

        IBindingSet[] t = new IBindingSet[chunk.length];

        int j = 0;

        for (int i = 0; i < chunk.length; i++) {

            final IBindingSet sourceSolution = chunk[i];
            
            IBindingSet bindingSet = sourceSolution;

            if (constraints != null
                    && !BOpUtility.isConsistent(constraints, bindingSet)) {

                continue;

            }

            if (selectVars != null) {

                bindingSet = bindingSet.copy(selectVars);

            }
            
            if (mergeSolution != null) {

                final Iterator<Map.Entry<IVariable, IConstant>> itr = mergeSolution
                        .iterator();

                while (itr.hasNext()) {

                    final Map.Entry<IVariable, IConstant> e = itr.next();

                    final IVariable var = e.getKey();

                    final IConstant val = e.getValue();

                    final IConstant oval = bindingSet.get(var);

                    if (oval != null) {
                        /*
                         * This is a paranoia assertion. We should never be in a
                         * position where merging in a binding could overwrite
                         * an existing binding with an inconsistent value.
                         */
                        if (!oval.equals(val)) {
                            throw new AssertionError(
                                    "Inconsistent binding: var=" + var
                                            + ", oval=" + oval + ", nval="
                                            + val);
                        }
                    } else {
                        bindingSet.set(var, val);
                    }

                }

//                log.error("\n    selectVars: " + Arrays.toString(selectVars)//
//                        + "\n mergeSolution: " + mergeSolution//
//                        + "\nsourceSolution: " + bindingSet//
//                        + "\noutputSolution: " + bindingSet//
//                        );

            }

            t[j++] = bindingSet;

        }

        if (j != chunk.length) {

            // allocate exact size array.
            final IBindingSet[] tmp = new IBindingSet[j];
//            final IBindingSet[] tmp = (IBindingSet[]) java.lang.reflect.Array
//                    .newInstance(chunk[0].getClass(), j);

            // make a dense copy.
            System.arraycopy(t/* src */, 0/* srcPos */, tmp/* dst */,
                    0/* dstPos */, j/* len */);

            t = tmp;

        }

        return t;

    }

//	/**
//	 * Inject (or replace) an {@link Integer} "rowId" column. This does not have
//	 * a side-effect on the source {@link IBindingSet}s.
//	 * 
//	 * @param var
//	 *            The name of the column.
//	 * @param start
//	 *            The starting value for the identifier.
//	 * @param in
//	 *            The source {@link IBindingSet}s.
//	 * 
//	 * @return The modified {@link IBindingSet}s.
//	 */
//	public static IBindingSet[] injectRowIdColumn(final IVariable<?> var,
//			final int start, final IBindingSet[] in) {
//
//		if (in == null)
//			throw new IllegalArgumentException();
//		
//		final IBindingSet[] out = new IBindingSet[in.length];
//		
//		for (int i = 0; i < out.length; i++) {
//		
//			final IBindingSet bset = in[i].clone();
//
//			bset.set(var, new Constant<Integer>(Integer.valueOf(start + i)));
//
//			out[i] = bset;
//			
//		}
//		
//		return out;
//
//	}

    /**
     * Return an ordered array of the bopIds associated with an ordered array of
     * predicates (aka a join path).
     * 
     * @param path
     *            A join path.
     * 
     * @return The ordered array of predicates for that join path.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalArgumentException
     *             if any element of the argument is <code>null</code>.
     * @throws IllegalStateException
     *             if any {@link IPredicate} does not have a defined bopId as
     *             reported by {@link BOp#getId()}.
     */
    public static int[] getPredIds(final IPredicate<?>[] path) {

        final int[] b = new int[path.length];
        
        for (int i = 0; i < path.length; i++) {
        
            b[i] = path[i].getId();
            
        }
        
        return b;

    }

    /**
     * Return the variable references shared by two operators. All variables
     * spanned by either {@link BOp} are considered, regardless of whether they
     * appear as operands or within annotations.
     * 
     * @param p
     *            An operator.
     * @param c
     *            Another operator.
     * 
     * @return The variable(s) in common. This may be an empty set, but it is
     *         never <code>null</code>.
     * 
     * @throws IllegalArgumentException
     *             if the two either reference is <code>null</code>.
     */
    public static Set<IVariable<?>> getSharedVars(final BOp p, final BOp c) {

        if (p == null)
            throw new IllegalArgumentException();

        if (c == null)
            throw new IllegalArgumentException();

        /*
         * Note: This is allowed since both arguments might be the same variable
         * or constant.
         */
//        if (p == c)
//            throw new IllegalArgumentException();

        // Collect the variables appearing anywhere in [p].
        Set<IVariable<?>> p1vars = null;
        {

            final Iterator<IVariable<?>> itr = BOpUtility
                    .getSpannedVariables(p);

            while (itr.hasNext()) {

            	if(p1vars == null) {
            		 
            		// lazy initialization.
            		p1vars = new LinkedHashSet<IVariable<?>>();
            		
            	}
            	
                p1vars.add(itr.next());

            }

        }

		if (p1vars == null) {
		
			// Fast path when no variables in [p].
			return Collections.emptySet();
			
		}
        
        // The set of variables which are shared.
        Set<IVariable<?>> sharedVars = null;

        // Consider the variables appearing anywhere in [c].
        {

            final Iterator<IVariable<?>> itr = BOpUtility
                    .getSpannedVariables(c);

            while (itr.hasNext()) {

                final IVariable<?> avar = itr.next();

                if (p1vars.contains(avar)) {
                    
					if (sharedVars == null) {

						// lazy initialization.
						sharedVars = new LinkedHashSet<IVariable<?>>();
						
					}
					
					sharedVars.add(avar);

                }

            }

        }

		if (sharedVars == null)
			return Collections.emptySet();

		return sharedVars;

    }

    /**
     * Return a copy of the subquery in which each {@link IAggregate} function
     * is a distinct instance. This prevents inappropriate sharing of state
     * across invocations of the subquery.
     * 
     * @param subQuery
     * @return
     */
    public static PipelineOp makeAggregateDistinct(PipelineOp subQuery) {

        return (PipelineOp) makeAggregateDistinct2(subQuery);
        
    }
    
    private static BOp makeAggregateDistinct2(final BOp op) {

        boolean dirty = false;
        
        /*
         * Children.
         */
        final int arity = op.arity();

        final BOp[] args = arity == 0 ? BOp.NOARGS : new BOp[arity];
        
        for (int i = 0; i < arity; i++) {

            final BOp child = op.get(i);

            // depth first recursion.
            args[i] = makeAggregateDistinct2(child);
            
            if(args[i] != child)
                dirty = true;
            
        }
        
        /*
         * Annotations.
         */
        
        final LinkedHashMap<String,Object> anns = new LinkedHashMap<String, Object>();
        
        for(Map.Entry<String, Object> e : op.annotations().entrySet()) {
            
            final String name = e.getKey();
            
            final Object oval = e.getValue();
            
            final Object nval;
            
            if(name.equals(GroupByOp.Annotations.GROUP_BY_REWRITE)) {
                
                nval = new GroupByRewriter((IGroupByRewriteState) oval);
                
                dirty = true;
                
            } else {
                
                nval = oval;
                
            }
            
            anns.put(name, nval);
            
        }

        if(!dirty)
            return op;
        
        try {

            @SuppressWarnings("unchecked")
            final Constructor<BOp> ctor = (Constructor<BOp>) op.getClass()
                    .getConstructor(BOp[].class, Map.class);
            
            return ctor.newInstance(args, anns);
            
        } catch (Exception e1) {
            
            throw new RuntimeException(e1);
            
        }
        
    }

    /**
     * Deep copy.  All bops will be distinct instances having the same data
     * for their arguments and their annotations.  Annotations which are 
     * bops are also deep copied.
     */
    public static <T extends BOp> T deepCopy(final T op) {

        if( op == null )
            return op;
        
        if (op instanceof IVariableOrConstant<?>) {
            /*
             * Note: Variables are immutable and support reference testing.
             * 
             * Note: Constants are immutable. There is a constant with an
             * annotation for the name of the corresponding variable, but the
             * constant can not be modified.
             */
            return op;
        }

        if(op instanceof BOpBase) {
            /*
             * The instances of this class can not be modified so we do not need
             * to do a deep copy.
             */
            return op;
        }
        
        /*
         * Children.
         */
        final int arity = op.arity();

        final BOp[] args = arity == 0 ? BOp.NOARGS : new BOp[arity];
        
        for (int i = 0; i < arity; i++) {

            final BOp child = op.get(i);

            // depth first recursion.
            args[i] = deepCopy(child);
            
        }
        
        /*
         * Annotations.
         */
        
        final LinkedHashMap<String,Object> anns = new LinkedHashMap<String, Object>();
        
        for(Map.Entry<String, Object> e : op.annotations().entrySet()) {
            
            final String name = e.getKey();
            
            final Object oval = e.getValue();
            
            final Object nval;
            
            if(oval instanceof BOp) {
                
                nval = deepCopy((BOp)oval);
                
            } else {
                
                nval = oval;
                
            }
            
            anns.put(name, nval);
            
        }

        try {

            @SuppressWarnings("unchecked")
            final Constructor<T> ctor = (Constructor<T>) op.getClass()
                    .getConstructor(BOp[].class, Map.class);

            final T copy = ctor.newInstance(args, anns);
            
            if (copy instanceof GroupNodeBase<?>) {
                
                /*
                 * Patch up the parent references.
                 */
                
                for (int i = 0; i < arity; i++) {

                    final IGroupMemberNode child = (IGroupMemberNode) copy
                            .get(i);
                    
                    child.setParent((GroupNodeBase<IGroupMemberNode>) copy);
                    
                }
                
            }
            
            return copy;

        } catch (Exception e1) {

            throw new RuntimeException(e1);

        }

    }

    /**
     * Combine two arrays of constraints. Either may be <code>null</code> or
     * empty. If both were <code>null</code> or empty, then a <code>null</code>
     * will be returned.
     * 
     * @param a
     *            One set of constraints.
     * @param b
     *            Another set of constraints.
     * 
     * @return The combined constraints.
     */
    static public IConstraint[] concat(final IConstraint[] a,
            final IConstraint[] b) {

        final List<IConstraint> list = new LinkedList<IConstraint>();
        
        if (a != null) {
            for (IConstraint c : a) {
                list.add(c);
            }
        }
        
        if (b != null) {
            for (IConstraint c : b) {
                list.add(c);
            }
        }
        
        return list.isEmpty() ? null : list
                .toArray(new IConstraint[list.size()]);

    }
    
    
    /**
     * Counts the number of occurrences of a BOp inside another BOp.
     * 
     * @param op the BOp to scan for occurrences of inner
     * @param inner the nested BOp we're looking for
     * @return the number of occurrences
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static int countVarOccurrencesOutsideProjections(
       final BOp op, final IVariable inputVar) {
       
       if (op==null || inputVar==null) {
          return 0; // no occurrences 
       }

       int innerNodesTotal = 0;
       final List<IVariable> vars = toList(op, IVariable.class);
       for (IVariable var : vars) {
          if (inputVar.equals(var))
             innerNodesTotal++;
       }
       
       int innerNodesInProj = 0;
       final List<ProjectionNode> projs = toList(op, ProjectionNode.class);
       for (ProjectionNode proj : projs) {
          final List<IVariable> innerVars = toList(proj, IVariable.class);
          for (IVariable innerVar : innerVars) {
             if (inputVar.equals(innerVar))
                innerNodesInProj++;
          }
       }

       return innerNodesTotal - innerNodesInProj;

   }

    

}
