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
package com.bigdata.journal;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * Transient buffer strategy uses a direct buffer but never writes on disk.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see BufferMode#Transient
 */
public class TransientBufferStrategy extends BasicBufferStrategy {
    
    /**
     * The root blocks.
     */
    final private IRootBlockView rootBlocks[] = new IRootBlockView[2];
    
    /**
     * Either zero (0) or one (1).
     */
    private int currentRootBlock = 0;
    
    TransientBufferStrategy(int offsetBits,long initialExtent, long maximumExtent,
            boolean useDirectBuffers) {
        
        /*
         * Note: I have not observed much performance gain from the use of
         * a direct buffer for the transient mode.
         */
        super(  maximumExtent,
                offsetBits,
                0/* nextOffset */, //
                0/*headerSize*/, //
                initialExtent, //
                BufferMode.Transient, //
                (useDirectBuffers ? ByteBuffer
                        .allocateDirect((int) initialExtent) : ByteBuffer
                        .allocate((int) initialExtent)));
        
    }
    
    public void deleteFile() {
        
        if( isOpen() ) {
            
            throw new IllegalStateException();
            
        }

        // NOP.
        
    }
    
    public void force(boolean metadata) {
        
        // NOP.
        
    }
    
    /**
     * Always returns <code>null</code>.
     */
    public File getFile() {
        
        return null;
        
    }

    public void closeAndDelete() {
        
        close();

    }

    final public boolean isStable() {
        
        return false;
        
    }

    public boolean isFullyBuffered() {
        
        return true;
        
    }
    
    public void writeRootBlock(IRootBlockView rootBlock, ForceEnum forceOnCommit) {
        
        if(rootBlock == null) throw new IllegalArgumentException();
        
        currentRootBlock = rootBlock.isRootBlock0() ? 0 : 1;

        rootBlocks[currentRootBlock] = rootBlock;
        
    }

}
