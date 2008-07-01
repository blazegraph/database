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

package com.bigdata.relation.rdf;

import java.util.Arrays;

import com.bigdata.relation.IRelationName;
import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;
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
    
    private IRelationName<SPO> focusStore;
    
    /**
     * De-serialization ctor.
     */
    public MappedProgram() {
        
        super();
        
    }
    
    /**
     * @param name
     * @param focusStore
     *            Identifies the relation containing the data to be asserted or
     *            retracted (optional). When <code>null</code> the steps are
     *            not transformed as they are added to the program.
     * @param parallel
     * @param closure
     */
    public MappedProgram(String name, IRelationName<SPO> focusStore, boolean parallel, boolean closure) {
        
        super(name, parallel, closure);
        
        this.focusStore = focusStore;
        
    }

    /**
     * Extended to add the N steps that map the given <i>step</i> across the
     * database and the focusStore.
     */
    public void addStep(IProgram step) {
        
        if(focusStore==null) {

            super.addStep(step);
            
        } else {
        
            addSteps( TMUtility.INSTANCE.mapProgramForTruthMaintenance(step, focusStore).steps() );
            
        }
        
    }

    /**
     * Adds a sub-program consisting of the fixed point closure of the given
     * rules.
     * 
     * @param rules The rules.
     */
    public void addClosureOf(IRule[] rules) {
        
        if (rules == null)
            throw new IllegalArgumentException();

        if (rules.length == 0)
            throw new IllegalArgumentException();
        
        final Program subProgram = new Program("closure", true/* parallel */,
                true/* true */);

        // add the rules whose closure will be computed into the sub-program.
        subProgram.addSteps(Arrays.asList(rules).iterator());

        /*
         * Add the sub-program to this program.
         * 
         * Note: it will be mapped for truth maintenance if a focus store was
         * specified.
         */
        addStep(subProgram);
        
    }

    /**
     * Adds a sub-program consisting of the fixed point closure of the given
     * rule.
     * 
     * @param rule The rule.
     */
    public void addClosureOf(IRule rule) {
        
        if (rule == null)
            throw new IllegalArgumentException();
        
        addClosureOf(new IRule[] { rule });
        
    }
    
}
