/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.rawstore;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.bigdata.counters.CounterSet;
import com.bigdata.mdi.IResourceMetadata;

/**
 * Simple delegation pattern.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class RawStoreDelegate implements IRawStore {

    protected final IRawStore delegate;
    
    public RawStoreDelegate(final IRawStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void delete(long addr) {
        delegate.delete(addr);
    }

    @Override
    public void deleteResources() {
        delegate.deleteResources();
    }

    @Override
    public void destroy() {
        delegate.destroy();
    }

    @Override
    public void force(boolean metadata) {
        delegate.force(metadata);
    }

    @Override
    public int getByteCount(long addr) {
        return delegate.getByteCount(addr);
    }

    @Override
    public CounterSet getCounters() {
        return delegate.getCounters();
    }

    @Override
    public File getFile() {
        return delegate.getFile();
    }

    @Override
    public long getOffset(long addr) {
        return delegate.getOffset(addr);
    }

    @Override
    public long getPhysicalAddress(final long addr) {
        return delegate.getPhysicalAddress(addr);
    }

    @Override
    public IResourceMetadata getResourceMetadata() {
        return delegate.getResourceMetadata();
    }

    @Override
    public UUID getUUID() {
        return delegate.getUUID();
    }

    @Override
    public boolean isFullyBuffered() {
        return delegate.isFullyBuffered();
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public boolean isReadOnly() {
        return delegate.isReadOnly();
    }

    @Override
    public boolean isStable() {
        return delegate.isStable();
    }

    @Override
    public ByteBuffer read(long addr) {
        return delegate.read(addr);
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public long toAddr(int nbytes, long offset) {
        return delegate.toAddr(nbytes, offset);
    }

    @Override
    public String toString(long addr) {
        return delegate.toString(addr);
    }

    @Override
    public long write(ByteBuffer data) {
        return delegate.write(data);
    }

    @Override
    public IPSOutputStream getOutputStream() {
        return delegate.getOutputStream();
    }

    @Override
    public InputStream getInputStream(long addr) {
        return delegate.getInputStream(addr);
    }

}
