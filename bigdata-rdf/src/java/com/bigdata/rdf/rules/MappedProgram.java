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

import com.bigdata.rdf.spo.SPO;
import com.bigdata.relation.IRelationIdentifier;
import com.bigdata.relation.rule.IProgram;
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
    
    private IRelationIdentifier<SPO> focusStore;
    
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
    public MappedProgram(String name, IRelationIdentifier<SPO> focusStore, boolean parallel, boolean closure) {
        
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
    
}
