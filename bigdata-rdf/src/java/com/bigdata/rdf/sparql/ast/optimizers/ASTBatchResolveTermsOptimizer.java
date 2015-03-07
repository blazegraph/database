/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Optimizer attempts to resolve any {@link BigdataValue}s in the AST which are
 * associated with a mock IV. This resolution step is normally performed by the
 * parser. However, when there is a sequence of UPDATE operations in the same
 * request, previous UPDATE operations in that sequence MIGHT have caused terms
 * to become declared in the lexicon so we must attempt to re-resolve any which
 * are mock IVs.
 * 
 * TODO We could have this always run and not do resolution in
 * {@link Bigdata2ASTSPARQLParser}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: BatchRDFValueResolver.java 6160 2012-03-18 19:57:37Z
 *          thompsonbry $
 */
public class ASTBatchResolveTermsOptimizer implements IASTOptimizer {

	private static final transient Logger log = Logger
			.getLogger(ASTBatchResolveTermsOptimizer.class);
	
	@Override
	public IQueryNode optimize(final AST2BOpContext context,
			final IQueryNode queryNode, final IBindingSet[] bindingSets) {

		/*
		 * Look for unknown terms and attempt to resolve them now.
		 */

		/*
		 * The list of the constants with mock IVs and null unless we find
		 * something.
		 * 
		 * Note: This is a list and not a set because we need to swap in the
		 * resolve IValueExpression for each ConstantNode in the query (there
		 * can be distinct ConstantNodes for the same unknown value and we would
		 * miss the duplicates unless this is a simple list).
		 */
		List<ConstantNode> unknown = null;

		// Look for unknown terms.
		{

			final Iterator<ConstantNode> itr = BOpUtility.visitAll(
					(BOp) queryNode, ConstantNode.class);

			while (itr.hasNext()) {

				final ConstantNode cnode = itr.next();

				final IConstant<?> c = cnode.getValueExpression();

				final IV<?, ?> iv = (IV<?, ?>) c.get();

				if (iv.isNullIV()) {

					if (unknown == null) {

						// Lazy allocation.
						unknown = new LinkedList<ConstantNode>();

					}

					unknown.add(cnode);

				}

			}

		}

		if (unknown != null) {

			/*
			 * Batch resolve any terms which are currently unknown.
			 */

			final BigdataValue[] values = new BigdataValue[unknown.size()];
			{
			
				/*
				 * First, build up the set of unknown terms.
				 */
				int i = 0;
				
				for (ConstantNode cnode : unknown) {

					final IConstant<?> c = cnode.getValueExpression();

					final IV<?, ?> iv = (IV<?, ?>) c.get();

					final BigdataValue v = iv.getValue();

					v.clearInternalValue();

					values[i++] = v;
					
				}

				/*
				 * Batch resolution.
				 */
				
				if (log.isDebugEnabled())
					log.debug("UNKNOWNS: " + Arrays.toString(values));

				context.getAbstractTripleStore().getLexiconRelation()
						.addTerms(values, values.length, true/* readOnly */);

			}

			/*
			 * Replace the value expression with one which uses the resolved IV.
			 */

			{
				int i = 0;
				for (ConstantNode cnode : unknown) {

					final BigdataValue v = values[i++];

					if (v.isRealIV()) {

						if (log.isInfoEnabled())
							log.info("RESOLVED: " + v + " => " + v.getIV());

						/*
						 * Note: If the constant is an effective constant
						 * because it was given in the binding sets then we also
						 * need to capture the variable name associated with
						 * that constant.
						 */

						final IV<?, ?> iv = v.getIV();

						final Constant<?> oldc = (Constant<?>) cnode
								.getValueExpression();

						final IV<?, ?> oldIv = (IV<?, ?>) oldc.get();

						if(oldIv.hasValue()) {

                            /*
                             * Propagate the cached Value onto the resolved IV.
                             * 
                             * @see
                             * https://sourceforge.net/apps/trac/bigdata/ticket
                             * /567 (Failure to set cached value on IV results
                             * in incorrect behavior for complex UPDATE
                             * operation)
                             */
                            ((IV) iv).setValue(oldIv.getValue());

						}

						final IVariable<?> var = (IVariable<?>) oldc
								.getProperty(Constant.Annotations.VAR);

						@SuppressWarnings({ "unchecked", "rawtypes" })
						final Constant<?> newc = (var == null ? new Constant(iv)
								: new Constant(var, iv));

						// cnode.setValueExpression(newc);
						cnode.setArg(0, newc);

					}

				}

			}

		}

		return queryNode;

	}

}
