package com.bigdata.ha;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.journal.IRootBlockView;

public interface IHALogReader {
	
	void close() throws IOException;
	
	boolean isEmpty();
	
	IRootBlockView getClosingRootBlock() throws IOException;
	
	boolean hasMoreBuffers() throws IOException;
	
	IHAWriteMessage processNextBuffer(final ByteBuffer clientBuffer) throws IOException;

}
