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
 * Created on Sep 27, 2007
 */

package com.bigdata.journal;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.bigdata.test.ExperimentDriver;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BasicExperimentConditions extends ExperimentDriver {

    /**
     * Sets up a series of {@link Condition}s based on the use of different
     * {@link BufferMode}s and also sets up {@link Condition}s for
     * {@link BufferMode}s that are backed by disk where
     * {@link Options#FORCE_ON_COMMIT} is set to {@link ForceEnum#No}
     */
    static public List<Condition> getBasicConditions(
            Map<String, String> properties, NV[] params) throws Exception {

        List<Condition> conditions = new LinkedList<Condition>();
        
        conditions.add(new Condition(properties));
        
        conditions = apply(
                conditions,
                new NV[][] { //
                        new NV[] { new NV(Options.BUFFER_MODE,
                                BufferMode.Transient), }, //
                        new NV[] { new NV(Options.BUFFER_MODE,
                                BufferMode.Direct), }, //
                        new NV[] {
                                new NV(Options.BUFFER_MODE, BufferMode.Direct),
                                new NV(Options.FORCE_ON_COMMIT, ForceEnum.No
                                        .toString()), }, //
                        new NV[] { new NV(Options.BUFFER_MODE, BufferMode.Mapped), }, //
                        new NV[] { new NV(Options.BUFFER_MODE, BufferMode.Disk), }, //
                        new NV[] {
                                new NV(Options.BUFFER_MODE, BufferMode.Disk),
                                new NV(Options.FORCE_ON_COMMIT, ForceEnum.No
                                        .toString()), }, //
                });
        
        return conditions;
        
//      properties = new HashMap<String,String>(properties);
//      
//      for(int i=0; i<params.length; i++) {
//          
//          properties.put(params[i].name,params[i].value);
//          
//      }

//        Condition[] conditions = new Condition[] { //
//                getCondition(properties, new NV[] { //
//                        new NV(Options.BUFFER_MODE, BufferMode.Transient), //
//                        }), //
////                getCondition(
////                        properties,
////                        new NV[] { //
////                                new NV(Options.BUFFER_MODE,
////                                        BufferMode.Transient), //
////                                new NV(Options.USE_DIRECT_BUFFERS, Boolean.TRUE) //
////                        }), //
//                getCondition(properties, new NV[] { //
//                        new NV(Options.BUFFER_MODE, BufferMode.Direct), //
//                        }), //
////                getCondition(
////                        properties,
////                        new NV[] { //
////                                new NV(Options.BUFFER_MODE, BufferMode.Direct), //
////                                new NV(Options.USE_DIRECT_BUFFERS, Boolean.TRUE) //
////                        }), //
//                getCondition(properties, new NV[] { //
//                        new NV(Options.BUFFER_MODE, BufferMode.Direct), //
//                                new NV(Options.FORCE_ON_COMMIT, ForceEnum.No) //
//                        }), //
////                getCondition(properties, new NV[] { //
////                        new NV(Options.BUFFER_MODE, BufferMode.Mapped), //
//////                        new NV(Options.FORCE_ON_COMMIT, ForceEnum.No) //
////                }), //
//                getCondition(properties, new NV[] { //
//                        new NV(Options.BUFFER_MODE, BufferMode.Disk), //
//                        }), //
//                getCondition(properties, new NV[] { //
//                        new NV(Options.BUFFER_MODE, BufferMode.Disk), //
//                                new NV(Options.FORCE_ON_COMMIT, ForceEnum.No) //
//                        }), //
//        };
//        
//        return Arrays.asList(conditions);

    }
    
}
