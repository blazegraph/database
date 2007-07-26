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
 * Created on Oct 18, 2006
 */

package com.bigdata.journal;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.bigdata.rawstore.Addr;

/**
 * Interface for a root block on the journal. The root block provides metadata
 * about the journal. The journal has two root blocks. The root blocks are
 * written in an alternating order and include timestamps at the head and tail
 * of each block (i.e., according to the Challis algorithm). On restart, the
 * root block is choosen whose (a) timestamps agree; and (b) whose timestamps
 * are greater. This protected against both crashes and partial writes of the
 * root block itself.
 * <p>
 * Note that some file systems or disks can re-order writes of by the
 * application and write the data in a more efficient order. This can cause the
 * root blocks to be written before the application data is stable on disk. The
 * {@link Options#DOUBLE_SYNC} option exists to defeat this behavior and ensure
 * restart-safety for such systems.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRootBlockView {

    /**
     * Assertion throws exception unless the root block is valid. Conditions
     * tested include the root block MAGIC and the root block timestamps (there
     * are two and they must agree).
     */
    public void valid() throws RootBlockException;

    /**
     * There are two root blocks and they are written in an alternating order.
     * For the sake of distinction, the first one is referred to as "rootBlock0"
     * while the 2nd one is referred to as "rootBlock1". This method indicates
     * which root block is represented by this view based on metadata supplied
     * to the constructor (the distinction is not persistent on disk).
     * 
     * @return True iff the root block view was constructed from "rootBlock0".
     */
    public boolean isRootBlock0();

    /**
     * The root block version number.
     */
    public int getVersion();
    
    /**
     * The next offset at which a data item would be written on the store.
     */
    public int getNextOffset();
    
    /**
     * The commit time of the earliest commit on the store or 0L iff there have
     * been no commits.
     * 
     * @return The earliest commit time on the store or 0L iff there have been
     *         no commits.
     */
    public long getFirstCommitTime();
    
    /**
     * The commit time of the most recent commit on the store or 0L iff there
     * have been no commits.
     * 
     * @return The commit time of the most recent commit on the store or 0L iff
     *         there have been no commits.
     * 
     * @todo this is the same as {@link #getCommitTimestamp()}
     */
    public long getLastCommitTime();
    
    /**
     * The timestamp at which the root block was last modified - this is a
     * purely local timestamp assigned internally when the root block is written
     * as part of the Challis algorithm. It is NOT the same as the
     * {@link #getCommitTimestamp() commit timestamp}.
     * 
     * @return The timestamp.
     * 
     * @throws RootBlockException
     *             if the timestamps on the root block do not agree.
     * 
     * @see #getCommitTimestamp()
     */
    public long getRootBlockTimestamp() throws RootBlockException;

    /**
     * The timestamp assigned to the commit by the commit protocol. This may be
     * assigned by a centralized time server in a distributed database. This
     * timestamp is NOT the same as the
     * {@link #getRootBlockTimestamp() root block timestamp}.
     * 
     * @return The commit timestamp.
     * 
     * @todo this the same as {@link #getLastCommitTime()}.
     */
    public long getCommitTimestamp();

    /**
     * A commit counter. This commit counter is used to avoid problems with
     * timestamps generated by different machines or when time goes backwards or
     * other nasty stuff. The correct root block is choosen by selecting the
     * valid root block with the larger commit counter.
     * 
     * @return The commit counter.
     */
    public long getCommitCounter();
    
    /**
     * Return the {@link Addr} at which the {@link ICommitRecord} for this root
     * block is stored. The {@link ICommitRecord}s are stored separately from
     * the root block so that they may be indexed by the
     * {@link #getCommitTimestamp()}. This is necessary in order to be able to
     * quickly recover the root addresses for a given commit timestamp, which is
     * a featured used to support transactional isolation.
     * <p>
     * Note: When a logical journal may overflow onto more than one physical
     * journal then the {@link Addr address} of the {@link ICommitRecord} MAY
     * refer to a historical physical journal and care MUST be exercised to
     * resolve the address against the appropriate journal file.
     * 
     * @return The {@link Addr address} at which the {@link ICommitRecord} for
     *         this root block is stored.
     */
    public long getCommitRecordAddr();

    /**
     * The {@link Addr address} of the root of the {@link CommitRecordIndex}.
     * The {@link CommitRecordIndex} contains the ordered {@link Addr addresses}
     * of the historical {@link ICommitRecord}s on the {@link Journal}. The
     * address of the {@link CommitRecordIndex} is stored directly in the root
     * block rather than the {@link ICommitRecord} since we can not obtain this
     * address until after we have formatted and written the
     * {@link ICommitRecord}.
     */
    public long getCommitRecordIndexAddr();
    
    /**
     * The unique journal identifier
     */
    public UUID getUUID();
    
    /**
     * A read-only buffer whose contents are the root block.
     */
    public ByteBuffer asReadOnlyBuffer();
    
}
