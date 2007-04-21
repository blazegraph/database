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
 * Created on Mar 18, 2007
 */

package com.bigdata.service;

import com.bigdata.rawstore.IRawStore;

/**
 * An interface used to pipeline writes against index partitions over one or
 * more secondary data services providing high availability and failover for
 * that index partition.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo define api for managing the write pipeline. any write operations on a
 *       data service should indicate the index partition (name + partitionId)
 *       on which they are writing and the write requests should be chained down
 *       the configured write pipeline.
 * 
 * @todo verify that conditional insert logic can not cause inconsistent data to
 *       appear depending on the order in which writes are received by the data
 *       services in a pipeline. For the pipeline to be consistent the order in
 *       which client operations execute MUST NOT differ on different data
 *       services in the pipeline for the same index partition. Consider that
 *       two clients are loading RDF documents whose terms overlap. If the order
 *       of the client operations differs on the different services for the
 *       pipeline, then different term identifiers could be assigned to the same
 *       term by different data services.
 * 
 * @todo commit (and group commit) semantics must be respected by the pipeline.
 *       this is basically a (potentially special) case of a 2-phase commit. If
 *       the, e.g., the last data services in the pipeline suddenly runs out of
 *       disk or otherwise "hiccups" then then it will not be consistent with
 *       the other replicas of the index partition in that pipeline. I need to
 *       think through how to handle this further. For example, the commit could
 *       propagate along the pipeline, but that does not work if group commit is
 *       triggered by different events (latency and data volumn) on different
 *       data services.
 *       <p>
 *       A distributed file system solves this problem by having only a single
 *       data service that is the chokepoint for concurrency control (and hence
 *       for consistency control) for any given index partition. Essentially,
 *       the distributed file system provides media redundency - the same bytes
 *       in the same order appear in each copy of the file backing the journal.
 *       <p>
 *       So, it seems that a way to achieve that without a distributed file
 *       system is to have the write pipeline operate and the {@link IRawStore}
 *       API. It simply streams writes down to the next service in the pipeline
 *       and that is the sole way in which downstream services have their stores
 *       written. Since low-level writes can be 1 GB/sec on a transient buffer,
 *       this protocol could be separated from the data service and become a
 *       media redundency protocol only. Downstream writes would not even need
 *       to sync to disk on "sync" but only on buffer overflow since the data
 *       service at the head of the pipeline is already providing restart-safe
 *       state and they are providing redundency for the first point of failure.
 *       If the data service does fail, then the first media redundency service
 *       would sync its state to disk and take over as the data service.
 *       <p>
 *       Work through how the service accepts responsibility for media
 *       redundency for files, how it names its local files (source host /
 *       filename?), how replicated files are managed (close, closeAndDelete,
 *       bulk read, syncToDisk, etc.)
 *       <p>
 *       Work through how index partitions can be shed or picked up by data
 *       services in this media redundency model.
 */
public interface IWritePipeline {

}
