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
 * Created on Aug 25, 2010
 */

package com.bigdata.bop.join;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.NV;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.controller.INamedSolutionSetRef;
import com.bigdata.bop.controller.NamedSetAnnotations;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.relation.accesspath.IBlockingBuffer;
import com.bigdata.relation.accesspath.UnsyncLocalOutputBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * This operator performs a nested loop join for solutions. Intermediate
 * solutions read from the pipeline are joined against a scan of some other
 * solution set. This operator is useful when the cardinality of the source
 * solutions in the pipeline is low (typically one empty source solution which
 * is exogenous to the query, but it is also cost efficient when there is a
 * small set of source solutions to be tested for each solution drained from the
 * named solution set). As the number of source solutions to be drained from the
 * pipeline grows, it eventually becomes cheaper to build a hash index over the
 * named solution set and perform a hash join with the source solutions.
 * <p>
 * Note: This operator MUST NOT reorder the solutions which are being scanned
 * from the named solution set. The query planner relies on that behavior to
 * optimize a SLICE from a pre-computed named solution set.
 * <p>
 * 
 * @see <a
 *      href="https://sourceforge.net/apps/mediawiki/bigdata/index.php?title=SPARQL_Update">
 *      SPARQL Update </a>
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/531"> SPARQL
 *      UPDATE for SOLUTION SETS </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: CopyOp.java 6010 2012-02-10 20:11:20Z thompsonbry $
 * 
 *          TODO This join does not implement optional semantics. If a use case
 *          for a nested loop join with optional semantics is identified at some
 *          point, then we will have to add support for optional here (and in
 *          the test suite).
 */
public class NestedLoopJoinOp extends PipelineOp {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends JoinAnnotations, NamedSetAnnotations {

//        /**
//		 * The name of the pre-existing named solution set to be scanned.
//		 */
//		String NAME = NestedLoopJoinOp.class.getName() + ".name";

    }

	/**
	 * Deep copy constructor.
	 * 
	 * @param op
	 */
	public NestedLoopJoinOp(final NestedLoopJoinOp op) {
		
		super(op);
		
	}

	/**
	 * Shallow copy constructor.
	 * 
	 * @param args
	 * @param annotations
	 */
	public NestedLoopJoinOp(final BOp[] args,
			final Map<String, Object> annotations) {

		super(args, annotations);

		// MUST be given.
		getRequiredProperty(Annotations.NAMED_SET_REF);

	}

	public NestedLoopJoinOp(final BOp[] args, final NV... annotations) {

		this(args, NV.asMap(annotations));

	}

//	/**
//	 * @see Annotations#NAME
//	 */
//	protected String getName() {
//		
//		return (String) getRequiredProperty(Annotations.NAME);
//		
//	}
	
    /**
     * @see Annotations#SELECT
     */
    protected IVariable<?>[] getSelect() {

        return getProperty(Annotations.SELECT, null/* defaultValue */);

    }

    /**
     * @see Annotations#CONSTRAINTS
     */
    protected IConstraint[] constraints() {

        return getProperty(Annotations.CONSTRAINTS, null/* defaultValue */);

    }

    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

        return new FutureTask<Void>(new ChunkTask(this, context));

    }

    /**
     * Copy the source to the sink.
     */
    static private class ChunkTask implements Callable<Void> {

        private final NestedLoopJoinOp op;

        private final BOpContext<IBindingSet> context;

        ChunkTask(final NestedLoopJoinOp op,
                final BOpContext<IBindingSet> context) {

            this.op = op;

            this.context = context;

        }

        @Override
        public Void call() throws Exception {

            final BOpStats stats = context.getStats();

            // Convert source solutions to array (assumes low cardinality).
            final IBindingSet[] leftSolutions = BOpUtility.toArray(
                    context.getSource(), stats);

            // default sink
            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();

            final UnsyncLocalOutputBuffer<IBindingSet> unsyncBuffer = new UnsyncLocalOutputBuffer<IBindingSet>(
                    op.getChunkCapacity(), sink);

            final IVariable<?>[] selectVars = op.getSelect();

            final IConstraint[] constraints = op.constraints();

            final ICloseableIterator<IBindingSet[]> ritr = getRightSolutions();
			
			try {

	            while (ritr.hasNext()) {

					final IBindingSet[] a = ritr.next();

					for (IBindingSet right : a) {

						for (IBindingSet left : leftSolutions) {

		                    // See if the solutions join. 
		                    final IBindingSet outSolution = BOpContext.bind(//
		                            right,//
		                            left,//
		                            constraints,//
		                            selectVars//
		                            );

							if (outSolution != null) {

								// Output the solution.
								unsyncBuffer.add(outSolution);
								
							}

						}
						
					}
					
				}
                
	            // flush the unsync buffer.
	            unsyncBuffer.flush();
                
                // flush the sink.
                sink.flush();

                // Done.
                return null;
                
            } finally {

                sink.close();
                
                context.getSource().close();
                
				if (ritr != null)
					ritr.close();

            }

        }

        /**
         * Return the right solutions.
         */
        protected ICloseableIterator<IBindingSet[]> getRightSolutions() {

            final INamedSolutionSetRef namedSetRef = (INamedSolutionSetRef) op
                    .getRequiredProperty(Annotations.NAMED_SET_REF);

            return context.getAlternateSource(namedSetRef);

        }

    } // class ChunkTask

}
