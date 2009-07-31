/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jul 1, 2008
 */

package com.bigdata.rdf.rules;

import java.util.Iterator;

import com.bigdata.relation.rule.IStep;
import com.bigdata.relation.rule.Program;

/**
 * Program automatically maps the rules added across the combination of the
 * database (the relation named on the rule) and the focusStore.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MappedProgram extends Program {

    /**
     * 
     */
    private static final long serialVersionUID = -2747355754941497325L;
    
    private String focusStore;
    
//    /**
//     * De-serialization ctor.
//     */
//    public MappedProgram() {
//        
//        super();
//        
//    }
    
    /**
     * @param name
     * @param focusStore
     *            Identifies the relation containing the data to be asserted or
     *            retracted (optional). When <code>null</code> the steps are
     *            not transformed as they are added to the program.
     * @param parallel
     * @param closure
     */
    public MappedProgram(String name, String focusStore, boolean parallel, boolean closure) {
        
        super(name, parallel, closure);
        
        this.focusStore = focusStore;
        
    }
    
    /**
     * Allow subclasses to use a different TM Utility.
     * 
     * @return TMUtility instance
     */
    protected TMUtility getTMUtility() {
        
        return TMUtility.INSTANCE;
        
    }

    /**
     * Extended to add the N steps that map the given <i>step</i> across the
     * database and the focusStore.
     */
    public void addStep(IStep step) {

        if (step == null)
            throw new IllegalArgumentException();
        
        if (focusStore == null) {

            super.addStep(step);

        } else {

            final Program subProgram = getTMUtility()
                    .mapForTruthMaintenance(step, focusStore);

            /*
             * FIXME I am not quite convinced that this is correct. The problems
             * appear when used with the "full" vs "fast" closure programs.
             * 
             * For the full closure program, we have a set of rules that we want
             * to fix point and we map those rules (individually) for truth
             * maintenance and fix point the resulting set of rules
             * 
             * For the fast closure program, we have a sequence of steps. Some
             * of those steps are closure operations. All of those steps need to
             * be mapped for truth maintenance (if the focusStore is specified).
             * 
             * There is a test suite for this (TestMappedProgram) but it does
             * not contain asserts and is not up to snuff.
             */
            if (this.isClosure()) {

                /*
                 * insert the individual rules into the closure program.
                 */
                
                final Iterator<? extends IStep> steps = subProgram.steps();

                while (steps.hasNext()) {

                    super.addStep(steps.next());

                }
            
            } else {

                // Note: invoke on super class to break recursion.
                super.addStep(subProgram);

            }

        }
        
    }
    
}
