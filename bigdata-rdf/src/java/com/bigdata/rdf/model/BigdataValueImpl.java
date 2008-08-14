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
 * Created on Apr 16, 2008
 */

package com.bigdata.rdf.model;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class BigdataValueImpl implements BigdataValue {

    private transient BigdataValueFactory valueFactory;

    private long termId;

    public final BigdataValueFactory getValueFactory() {
        
        return valueFactory;
        
    }
    
    /**
     * 
     * @param valueFactory
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>.
     * @throws IllegalStateException
     *             if a different {@link BigdataValueFactoryImpl} has already been
     *             set.
     */
    public final void setValueFactory(BigdataValueFactory valueFactory) {

        if (valueFactory == null)
            throw new IllegalArgumentException();

        if (this.valueFactory != null && this.valueFactory != valueFactory)
            throw new IllegalStateException();

        this.valueFactory = valueFactory;
        
    }
    
    /**
     * @param valueFactory
     *            The value factory that created this object (optional).
     * @param termId
     *            The term identifier (optional).
     */
    protected BigdataValueImpl(BigdataValueFactory valueFactory, long termId) {
        
//        if (valueFactory == null)
//            throw new IllegalArgumentException();
        
        this.valueFactory = valueFactory;
        
        this.termId = termId;
        
    }

    final public void clearTermId() {

        termId = NULL;
        
    }

    final public long getTermId() {

        return termId;
        
    }

    final public void setTermId(final long termId) {

        if (termId == NULL) {

            throw new IllegalArgumentException();

        }

        if (this.termId != NULL && this.termId != termId) {

            throw new IllegalStateException("termId already assigned: old="
                    + this.termId + ", new=" + termId);

        }
        
        this.termId = termId;
        
    }

}
