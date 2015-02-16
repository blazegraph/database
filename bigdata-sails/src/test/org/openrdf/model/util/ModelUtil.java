/* Licensed to Aduna under one or more contributor license agreements.  
 * See the NOTICE.txt file distributed with this work for additional 
 * information regarding copyright ownership. Aduna licenses this file
 * to you under the terms of the Aduna BSD License (the "License"); 
 * you may not use this file except in compliance with the License. See 
 * the LICENSE.txt file distributed with this work for the full License.
 *
 * Unless required by applicable law or agreed to in writing,software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
 * implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 */
package org.openrdf.model.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.util.iterators.Iterators;

/**
 * @author Arjohn Kampman
 */
public class ModelUtil {

	/**
	 * Compares two models, defined by two statement collections, and returns
	 * <tt>true</tt> if they are equal. Models are equal if they contain the same
	 * set of statements. Blank node IDs are not relevant for model equality,
	 * they are mapped from one model to the other by using the attached
	 * properties.
	 */
	public static boolean equals(Iterable<? extends Statement> model1, Iterable<? extends Statement> model2) {
		// Filter duplicates
		Set<Statement> set1 = new LinkedHashSet<Statement>();
		Iterators.addAll(model1.iterator(), set1);

		Set<Statement> set2 = new LinkedHashSet<Statement>();
		Iterators.addAll(model2.iterator(), set2);

		return equals(set1, set2);
	}

	/**
	 * Compares two models, defined by two statement collections, and returns
	 * <tt>true</tt> if they are equal. Models are equal if they contain the same
	 * set of statements. Blank node IDs are not relevant for model equality,
	 * they are mapped from one model to the other by using the attached
	 * properties.
	 */
	public static boolean equals(Set<? extends Statement> model1, Set<? extends Statement> model2) {
		// Compare the number of statements in both sets
		if (model1.size() != model2.size()) {
			return false;
		}

		return isSubsetInternal(model1, model2);
	}

	/**
	 * Compares two models, defined by two statement collections, and returns
	 * <tt>true</tt> if the first model is a subset of the second model.
	 */
	public static boolean isSubset(Iterable<? extends Statement> model1, Iterable<? extends Statement> model2)
	{
		// Filter duplicates
		Set<Statement> set1 = new LinkedHashSet<Statement>();
		Iterators.addAll(model1.iterator(), set1);

		Set<Statement> set2 = new LinkedHashSet<Statement>();
		Iterators.addAll(model2.iterator(), set2);

		return isSubset(set1, set2);
	}

	/**
	 * Compares two models, defined by two statement collections, and returns
	 * <tt>true</tt> if the first model is a subset of the second model.
	 */
	public static boolean isSubset(Set<? extends Statement> model1, Set<? extends Statement> model2) {
		// Compare the number of statements in both sets
		if (model1.size() > model2.size()) {
			return false;
		}

		return isSubsetInternal(model1, model2);
	}

	private static boolean isSubsetInternal(Set<? extends Statement> model1, Set<? extends Statement> model2) {
		// try to create a full blank node mapping
		return matchModels(model1, model2);
	}

	private static boolean matchModels(Set<? extends Statement> model1, Set<? extends Statement> model2) {
		// Compare statements without blank nodes first, save the rest for later
		List<Statement> model1BNodes = new ArrayList<Statement>(model1.size());

		for (Statement st : model1) {
			if (st.getSubject() instanceof BNode || st.getObject() instanceof BNode) {
				model1BNodes.add(st);
			}
			else {
				if (!model2.contains(st)) {
					return false;
				}
			}
		}

		return matchModels(model1BNodes, model2, new HashMap<BNode, BNode>(), 0);
	}

	/**
	 * A recursive method for finding a complete mapping between blank nodes in
	 * model1 and blank nodes in model2. The algorithm does a depth-first search
	 * trying to establish a mapping for each blank node occurring in model1.
	 * 
	 * @param model1
	 * @param model2
	 * @param bNodeMapping
	 * @param idx
	 * @return true if a complete mapping has been found, false otherwise.
	 */
	private static boolean matchModels(List<? extends Statement> model1, Iterable<? extends Statement> model2,
			Map<BNode, BNode> bNodeMapping, int idx)
	{
		boolean result = false;

		if (idx < model1.size()) {
			Statement st1 = model1.get(idx);

			List<Statement> matchingStats = findMatchingStatements(st1, model2, bNodeMapping);

			for (Statement st2 : matchingStats) {
				// Map bNodes in st1 to bNodes in st2
				Map<BNode, BNode> newBNodeMapping = new HashMap<BNode, BNode>(bNodeMapping);

				if (st1.getSubject() instanceof BNode && st2.getSubject() instanceof BNode) {
					newBNodeMapping.put((BNode)st1.getSubject(), (BNode)st2.getSubject());
				}

				if (st1.getObject() instanceof BNode && st2.getObject() instanceof BNode) {
					newBNodeMapping.put((BNode)st1.getObject(), (BNode)st2.getObject());
				}

				// FIXME: this recursive implementation has a high risk of
				// triggering a stack overflow

				// Enter recursion
				result = matchModels(model1, model2, newBNodeMapping, idx + 1);

				if (result == true) {
					// models match, look no further
					break;
				}
			}
		}
		else {
			// All statements have been mapped successfully
			result = true;
		}

		return result;
	}

	private static List<Statement> findMatchingStatements(Statement st, Iterable<? extends Statement> model,
			Map<BNode, BNode> bNodeMapping)
	{
		List<Statement> result = new ArrayList<Statement>();

		for (Statement modelSt : model) {
			if (statementsMatch(st, modelSt, bNodeMapping)) {
				// All components possibly match
				result.add(modelSt);
			}
		}

		return result;
	}

	private static boolean statementsMatch(Statement st1, Statement st2, Map<BNode, BNode> bNodeMapping) {
		URI pred1 = st1.getPredicate();
		URI pred2 = st2.getPredicate();

		if (!pred1.equals(pred2)) {
			// predicates don't match
			return false;
		}

		Resource subj1 = st1.getSubject();
		Resource subj2 = st2.getSubject();

		if (subj1 instanceof BNode && subj2 instanceof BNode) {
			BNode mappedBNode = bNodeMapping.get(subj1);

			if (mappedBNode != null) {
				// bNode 'subj1' was already mapped to some other bNode
				if (!subj2.equals(mappedBNode)) {
					// 'subj1' and 'subj2' do not match
					return false;
				}
			}
			else {
				// 'subj1' was not yet mapped. we need to check if 'subj2' is a
				// possible mapping candidate
				if (bNodeMapping.containsValue(subj2)) {
					// 'subj2' is already mapped to some other value.
					return false;
				}
			}
		}
		else {
			// subjects are not (both) bNodes
			if (!subj1.equals(subj2)) {
				return false;
			}
		}

		Value obj1 = st1.getObject();
		Value obj2 = st2.getObject();

		if (obj1 instanceof BNode && obj2 instanceof BNode) {
			BNode mappedBNode = bNodeMapping.get(obj1);

			if (mappedBNode != null) {
				// bNode 'obj1' was already mapped to some other bNode
				if (!obj2.equals(mappedBNode)) {
					// 'obj1' and 'obj2' do not match
					return false;
				}
			}
			else {
				// 'obj1' was not yet mapped. we need to check if 'obj2' is a
				// possible mapping candidate
				if (bNodeMapping.containsValue(obj2)) {
					// 'obj2' is already mapped to some other value.
					return false;
				}
			}
		}
		else {
			// objects are not (both) bNodes
			if (!obj1.equals(obj2)) {
				return false;
			}
		}

		return true;
	}
}