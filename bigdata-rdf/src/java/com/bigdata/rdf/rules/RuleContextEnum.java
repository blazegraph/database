/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.rules;

import com.bigdata.btree.BTree;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.rdf.inf.TruthMaintenance;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.LocalTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
import com.bigdata.service.IBigdataFederation;

/**
 * <p>
 * Type-safe enumeration capturing the primary uses cases for rule execution.
 * </p>
 * <p>
 * The uses cases here reduce to two basic variants: (a) Query using a
 * read-consistent view; and (b) Rules that write on a database. The latter has
 * two twists: for {@link #TruthMaintenance} the rules write on a
 * {@link TempTripleStore} while for {@link #DatabaseAtOnceClosure} they write
 * directly on the knowledge base.
 * </p>
 * <p>
 * Note: The scale-out architecture imposes a concurrency control layer such
 * that conflicts for access to the unisolated indices can not arise and
 * therefore is not relevant to the rest of this discussion.
 * </p>
 * <p>
 * For the use cases that write on a database without the concurrency control
 * laer (regardless of whether it is the focusStore or the main knowledge base)
 * there is a concurrency control issue that can be resolved in one or two
 * different ways. The basic issue is that rule execution populates
 * {@link IBuffer}s that are automatically flushed when they become full (or
 * when a sequential step in an {@link IProgram} is complete). If there are
 * iterator(s) reading concurrently on the same view of the index on which the
 * buffer(s) write, then this violates the contract for the {@link BTree} which
 * is safe for concurrent readers -or- a single writer. The parallel execution
 * of more than one rule makes this a problem even with the iterators are fully
 * buffered (vs the newer asynchronous iterators which have the same problem
 * even when only one rule is running.)
 * </p>
 * <p>
 * Note: For {@link #TruthMaintenance} we actually read from two different
 * sources: a focusStore and the knowledge base. In this situation we are free
 * to read on the knowledge base using an unisolated view because truth
 * maintenance requires exclusive write access and therefore no other process
 * will be writing on the knowledge base.
 * </p>
 * <p>
 * We can do two things to avoid violating the {@link BTree} concurrency
 * contract:
 * <ol>
 * <li>Read using a read-committed view (for the source on which the rules will
 * write) and write on the unisolated view. The main drawback with this approach
 * is that we must checkpoint (for a {@link TemporaryStore}) or commit (for a
 * {@link Journal}) after each sequential step of an {@link IProgram} (including
 * after each round of closure as a special case). This slows down inference
 * and, for {@link TruthMaintenance}, can cause the {@link TemporaryStore} to be
 * flushed to disk when otherwise it might be fully buffered and never touch the
 * disk.</li>
 * <li>
 * <p>
 * Read and write on the unisolated {@link BTree} and use a mutex lock
 * coordinate access to that index. The mutex lock must serialize (concurrent)
 * readers and the (single) writer. The writer gains the lock when it needs to
 * flush a buffer, at which point any reader(s) on the unisolated {@link BTree}s
 * block and grant access to the writer and then resume their operations when
 * the writer releases the lock.
 * </p>
 * <p>
 * For a single rule, only an asynchronous iterator can conflict write the task
 * flushing the buffer. However, when more than one rule is being executed
 * concurrently, it is possible for conflicts to arise even with fully buffered
 * iterators.
 * </p>
 * <p>
 * The advantage of this approach is that we can use only the unisolated indices
 * (better buffer management) and we do not need to either checkpoint (for a
 * {@link TempTripleStore}) or commit (for a {@link LocalTripleStore}). For
 * {@link TempTripleStore} this can mean that we never even touch the disk while
 * for a {@link LocalTripleStore} is means that we only commit when the closure
 * operation is complete.
 * </p>
 * </li>
 * </ol>
 * </p>
 * 
 * @todo we have to jump through hoops whenever we are doing
 *       {@link #TruthMaintenance} with a focusStore backed by a
 *       {@link TemporaryStore} (which is the only way we can do it today).
 *       <p>
 *       For database at once closure, we only need to jump through hoops when
 *       the database is on a {@link Journal}. If it is on an
 *       {@link IBigdataFederation} then the concurrency control layer ensures
 *       that none of the problems can arise.
 * 
 * @todo we need to recognize the use case and then recognize which relations
 *       (and their indices) belong to the focusStore and the knowledge base so
 *       that we can choose the appropriate view for each.
 * 
 * @todo flushing the {@link IBuffer} for mutation operations needs to
 *       coordinate with both the fully buffered and the asynchronous iterators.
 *       this is only for {@link #TruthMaintenance} or when the knowledge base
 *       is on a {@link Journal}. there must be one mutex per named index on
 *       which we will write (actually, that can be simplified to one mutex per
 *       relation on which we will write since the relations always update all
 *       of their indices).
 * 
 * @todo Use the readTimestamp for query (so we can query for a historical
 *       commit time) but ignore it for {@link #DatabaseAtOnceClosure} and
 *       {@link #TruthMaintenance} (presuming that we are operating on the
 *       current state of the kb)?
 */
public enum RuleContextEnum {

	/**
	 * <p>
	 * Database at once closure is the most efficient way to compute the closure
	 * over the model theory for the KB. In general, database-at-once closure is
	 * requested when you bulk load a large amount of data into a knowledge
	 * base. You request database-at-once closure using
	 * {@link InferenceEngine#computeClosure(AbstractTripleStore)} WITHOUT the
	 * optional <i>focusStore</i>.
	 * </p>
	 * <p>
	 * As long as justifications are enabled, you can incrementally assert or
	 * retract statements using {@link #TruthMaintenance}. If justifications are
	 * NOT enabled, then you can re-compute the closure of the database after
	 * adding assertions. If you have retracted assertions, then you first need
	 * to delete all inferences from the knowledge base and then recompute the
	 * closure of the database.
	 * </p>
	 * <p>
	 * Database-at-once closure reads and writes on the persistent knowledge
	 * base and does not utilize a {@link TempTripleStore}.
	 * </p>
	 */
	DatabaseAtOnceClosure,

	/**
	 * <p>
	 * Truth maintenance must be used when you <em>incrementally</em> assert or
	 * retract a set of <em>explicit</em> (or told) statements (or assertions or
	 * triples). Each time new assertions are made or retracted the closure of
	 * the knowledge base must be updated, causing entailments (or
	 * <em>inferred</em> statements) to be either asserted or retracted. This is
	 * handled by {@link TruthMaintenance} and {@link InferenceEngine}.
	 * </p>
	 * <p>
	 * Adding assertions is relatively straight forward since all the existing
	 * entailments will remain valid, but new entailments might be computable
	 * based on the new assertions. The only real twist is that we record
	 * justifications (aka proof chains) to support truth maintenance when
	 * statements are retracted.
	 * </p>
	 * <p>
	 * Retractions require additional effort since entailments already in the
	 * knowledge base MIGHT NOT be supported once some explicit statements are
	 * retracted. Attempting to directly retract an inference or an axiom has no
	 * effect since they are entailments by some combination of the model theory
	 * and the explicit statements. However, when an explicit statement in the
	 * knowledge base is retracted a search must be performed to identify
	 * whether or not the statement is still provable based on the remaining
	 * statements. In the current implementation we chase justification in order
	 * to decide whether or not the explicit statement will be converted to an
	 * inference (or an axiom) or retracted from the knowledge base. This
	 * process is recursive since a statement that is gets retracted (rather
	 * than being converted to an inference) can cause other entailments to no
	 * longer be supported.
	 * </p>
	 * <p>
	 * When asserting or retracting statements using truth maintenance, the
	 * statements are first loaded into a {@link TempTripleStore} known as the
	 * <em>focusStore</em>. Next we compute the closure of the <i>focusStore</i>
	 * against the assertions already in the knowledge base. This is done using
	 * {@link TMUtility} to <em>rewrite</em> the {@link IProgram} into a new
	 * (and larger) set of rules. For each original {@link IRule}, we derive N
	 * new rules, where N is the number of tail {@link IPredicate} in the rule.
	 * These derived rules reads from either the <i>focusStore</i> or the fused
	 * view of the <i>focusStore</i> and the knowledge base and they
	 * <em>write</em> on the <i>focusStore</i>. Once the closure of the
	 * focusStore against the knowledge base has been computed, all statements
	 * in that closure are either asserted against or retracted from the
	 * knowledge base (depending on whether the original set of statements was
	 * being asserted or retracted). That final step is done using either a bulk
	 * statement copy or a bulk statement remove operation.
	 * </p>
	 * <p>
	 * Since the state of the knowledge base does not change while we are
	 * computing the closure of the focusStore against the knowledge base we can
	 * use a read-consistent view of the knowledge base throughout the
	 * operation. At the same time, we are both reading from and writing on the
	 * focusStore.
	 * </p>
	 */
	TruthMaintenance,

	/**
	 * <p>
	 * High-level queries (SPARQL) can in general be translated into a rule that
	 * is directly executed by the bigdata rule execution layer. This provides
	 * extremely efficient query answering. The same approach can be used with
	 * custom rule evaluation - there is no difference once it gets down to the
	 * execution of the rule(s).
	 * </p>
	 * <p>
	 * The generated rule SHOULD be executed against a <em>read-consistent</em>
	 * view of the knowledge base (NOT read-committed since that can result in
	 * dirty reads). In a scenario where the knowledge base is unchanging, this
	 * is very efficient as it allows full concurrency with less (no) overhead
	 * for concurrency control. In addition, concurrent writes on the knowledge
	 * base are allowed.
	 * </p>
	 * <p>
	 * New readers SHOULD use a read-consistent timestamp that reflects the
	 * desired (generally, most recent) commit point corresponding to a closure
	 * of the knowledge base.
	 * </p>
	 */
	HighLevelQuery;
	
}
