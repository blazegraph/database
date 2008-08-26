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
/*
 * Created on Oct 25, 2007
 */

package com.bigdata.rdf.rules;

import java.util.Set;

import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IRuleTaskFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.IStepTask;

public class RuleFastClosure3 extends AbstractRuleFastClosure_3_5_6_7_9 {

    /**
     * 
     */
    private static final long serialVersionUID = 8276555097415122677L;

    /**
     * @param vocab
     * @param P
     */
    public RuleFastClosure3(
            final String database,
            final String focusStore,
            final Vocabulary vocab) {
        //, Set<Long> P) {
        
        super("fastClosure3",
                database,
                vocab.getConstant(RDFS.SUBPROPERTYOF),
                vocab.getConstant(RDFS.SUBPROPERTYOF),
                /*
                 * Custom rule executor factory.
                 */
                new IRuleTaskFactory() {

                    public IStepTask newTask(IRule rule, IJoinNexus joinNexus,
                            IBuffer<ISolution> buffer) {

                        return new FastClosureRuleTask(database, focusStore,
                                rule, joinNexus, buffer, /* P, */
                                vocab.getConstant(RDFS.SUBPROPERTYOF), //
                                vocab.getConstant(RDFS.SUBPROPERTYOF)) {

                            /**
                             * Note: This is the set {P} in the fast closure
                             * program.
                             */
                            public Set<Long> getSet() {

                                return getSubProperties();

                            }

                        };

                    }

                });// , P);

    }

}