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
 * Created on Nov 17, 2006
 */
package com.bigdata.objndx;

/**
 * A persistent object.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo This abstract class is only used by the {@link BTree}. Modify it so
 *       that we directly test the member fields for better performance.
 */
abstract public class PO implements IIdentityAccess, IDirty {

    /**
     * The persistent identity (defined when the object is actually
     * persisted).
     */
    transient protected long identity = NULL;

    /**
     * True iff the object is deleted.
     */
    transient protected boolean deleted = false;
    
    final public boolean isPersistent() {

        return identity != NULL;

    }

    final public boolean isDeleted() {
        
        return deleted;
        
    }
    
    final public long getIdentity() throws IllegalStateException {

        if (identity == NULL) {
            
            throw new IllegalStateException();

        }

        return identity;

    }

    /**
     * Used by the store to set the persistent identity.
     * 
     * Note: This method should not be public.
     * 
     * @param identity
     *            The identity.
     * 
     * @throws IllegalStateException
     *             If the identity is already defined.
     */
    void setIdentity(long key) throws IllegalStateException {

        if (key == NULL) {

            throw new IllegalArgumentException();
            
        }

        if (this.identity != NULL) {

            throw new IllegalStateException("Object already persistent");
            
        }
        
        if( this.deleted) {
            
            throw new IllegalStateException("Object is deleted");
            
        }

        this.identity = key;

    }

    /**
     * New objects are considered to be dirty. When an object is
     * deserialized from the store the dirty flag MUST be explicitly
     * cleared.
     */
    transient protected boolean dirty = true;

    final public boolean isDirty() {

        return dirty;

    }

    public void setDirty(boolean dirty) {

        this.dirty = dirty;

    }

    /**
     * Extends the basic behavior to display the persistent identity of the
     * object iff the object is persistent and to mark objects that have been
     * deleted.
     */
    public String toString() {

        final String s;
        
        if (identity != NULL) {

            s = super.toString() + "#" + identity;

        } else {

            s = super.toString();

        }

        if (deleted) {

            return s + "(deleted)";

        } else {

            return s;

        }

    }

}
