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
package com.bigdata.scaleup;

import com.bigdata.objndx.IndexSegment;

/**
 * Interface for a scheduleable task that produces one or more
 * {@link IndexSegment}s, updates the {@link MetadataIndex} to reflect the
 * existence of the new {@link IndexSegment}s and notifies existing views
 * with a depedency on the source(s) that they must switch over to the new
 * {@link IndexSegment}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IPartitionTask extends
        java.util.concurrent.Callable<Object> {
    
    /**
     * Run the task.
     * 
     * @return No return semantics are defined.
     * 
     * @throws Exception
     *             The exception thrown by the task.
     */
    public Object call() throws Exception;

}