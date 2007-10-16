/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

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
