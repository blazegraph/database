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
 * Created on Mar 15, 2007
 */

package com.bigdata.service;

import java.io.Serializable;

import com.bigdata.btree.IIndexWithCounter;
import com.bigdata.btree.IReadOnlyOperation;

/**
 * An arbitrary index procedure to be executed on an {@link IDataService}.
 * <p>
 * Note: while this interface is {@link Serializable}, that provides only for
 * communicating state to the {@link IDataService}. If an instance of this
 * procedure will cross a network interface, then the implementation Class MUST
 * be available to the {@link IDataService} on which it will execute. This can
 * be as simple as bundling the procedure into a JAR that is part of the
 * CLASSPATH used to start a {@link DataService} or you can use downloaded code
 * with the JINI codebase mechanism.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see IReadOnlyOperation
 * 
 * @todo refactor into the btree package, abstract away from the data services,
 *       and remove the exception declaration for
 *       {@link #apply(IIndexWithCounter)}.
 */
public interface IProcedure extends Serializable {

    /**
     * Run the procedure.
     * <p>
     * Unisolated procedures have "auto-commit" ACID properties for the local
     * {@link IDataService} on which they execute, but DO NOT have distributed
     * ACID properties. In order for a distributed procedure to be ACID, the
     * procedure MUST be fully isolated.
     * 
     * @param ndx
     *            The index.
     * 
     * @return The result, which is entirely defined by the procedure
     *         implementation and which MAY be null. In general, this MUST be
     *         {@link Serializable} since it may have to pass across a network
     *         interface.
     */
    public Object apply(IIndexWithCounter ndx) throws Exception;
    
}
