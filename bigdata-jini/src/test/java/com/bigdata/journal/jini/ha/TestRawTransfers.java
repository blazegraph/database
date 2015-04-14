/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
package com.bigdata.journal.jini.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import junit.framework.TestCase;

import com.bigdata.ha.msg.HARebuildRequest;
import com.bigdata.ha.msg.HAWriteMessage;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.journal.BufferMode;
import com.bigdata.journal.FileMetadata;
import com.bigdata.journal.ForceEnum;
import com.bigdata.journal.IHABufferStrategy;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;
import com.bigdata.journal.RootBlockView;
import com.bigdata.util.ChecksumUtility;

/**
 * Shortcuts the HA transfer protocols to exercise directly the low level
 * data transfers at the BufferStrategy interface.
 * 
 * @author Martyn Cutcher
 *
 */
public class TestRawTransfers extends TestCase {
	
    private Random random;

    protected void setUp() throws Exception {
        super.setUp();
        random = new Random();
    }

    protected void tearDown() throws Exception {
        random = null;
        super.tearDown();
    }

	/**
	 * Create two stores:
	 * 	add data to one
	 * 	transfer raw
	 * 	read data from both
	 * 
	 * The plan is to create the messages and buffers directly, finessing the usual HA
	 * plumbing so as to be able to test the underlying transfer methods.
	 * @throws IOException 
	 */
	public void testSimpleStoreTransferRW() throws IOException {
		doSimpleStoreTransfer(BufferMode.DiskRW);
	}
	
	public void testSimpleStoreTransferWORM() throws IOException {
		doSimpleStoreTransfer(BufferMode.DiskWORM);
	}
	
	public void doSimpleStoreTransfer(final BufferMode mode) throws IOException {
		final Journal journal1 = getStore(mode);
		final Journal journal2 = getStore(mode);
		try {
			final ByteBuffer data = randomData();
			final long dataAddr = journal1.write(data);
			journal1.commit();

			final IHABufferStrategy src = (IHABufferStrategy) journal1
					.getBufferStrategy();
			final IHABufferStrategy dst = (IHABufferStrategy) journal2
					.getBufferStrategy();

			final HARebuildRequest req = new HARebuildRequest(UUID.randomUUID());
			final long extent = src.getExtent();
			final ByteBuffer transfer = ByteBuffer.allocate(1024 * 1024);
			long position = FileMetadata.headerSize0;

//			// AN HARebuild request must be sent to the destination resulting in
//			//	a prepareForRebuild request.
//			dst.prepareForRebuild(req);

			// transfer full file store
			long sequence = 0;
			while (position < extent) {
				int sze = transfer.capacity();
				if ((position+sze) > extent)
					sze = (int) (extent - position);
				
				transfer.position(0);
				transfer.limit(sze);
				src.readRaw(position, transfer);
				
                final IHAWriteMessage msg = new HAWriteMessage(
                        journal1.getUUID(), -1, -1, sequence++, sze,
                        -1/* chk */, mode.getStoreType(), -1 /* quorumToken */,
                        extent, position);
				
				dst.writeRawBuffer(req, msg, transfer);

				position += sze;

			}

			// now transfer root blocks
			final ChecksumUtility checker = new ChecksumUtility();

			final IRootBlockView rbv0 = new RootBlockView(true,
					src.readRootBlock(true), checker);
			final IRootBlockView rbv1 = new RootBlockView(false,
					src.readRootBlock(false), checker);
			
			dst.writeRootBlock(rbv0, ForceEnum.ForceMetadata);
			dst.writeRootBlock(rbv1, ForceEnum.ForceMetadata);
			
			final  IRootBlockView activeRbv = rbv0.getCommitCounter() > rbv1.getCommitCounter() ? rbv0 : rbv1;

//			dst.completeRebuild(req, activeRbv);
			dst.resetFromHARootBlock(activeRbv);
			
			// now see if we can read the data written to the first journal from the second
			data.position(0);
			assertEquals(data, journal1.read(dataAddr));
			assertEquals(data, journal2.read(dataAddr));
			assertEquals(journal1.read(dataAddr), journal2.read(dataAddr));
		} finally {
			journal1.destroy();
			journal2.destroy();
		}
		
	}
	
	private ByteBuffer randomData() {
		final byte[] buf = new byte[200 + random.nextInt(2000)];
		random.nextBytes(buf);
		
		return ByteBuffer.wrap(buf);
	}
	
	private Journal getStore(final BufferMode mode) {
		final Properties properties = new Properties();
		
        properties.setProperty(Options.CREATE_TEMP_FILE,"true");
        properties.setProperty(Options.DELETE_ON_EXIT,"true");
        properties.setProperty(Options.BUFFER_MODE, mode.toString());
        		
		return new Journal(properties);
	}
}
