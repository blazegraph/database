package com.bigdata.journal.ha;

import com.bigdata.io.ObjectSocketChannelStream;
import com.bigdata.io.WriteCache;
import com.bigdata.io.WriteCacheService;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.Environment;
import com.bigdata.journal.IBufferStrategy;

/**
 * Provides two interfaces, from both HAGlue RMI and the IHAClient
 * 
 * @author Martyn Cutcher
 *
 */
public abstract class HAClient extends HADelegate implements IHAClient {

	public HAClient(Environment environment) {
		super(environment);
		// TODO Auto-generated constructor stub
	}

	public ObjectSocketChannelStream getInputSocket() {
		// TODO Auto-generated method stub
		return null;
	}

	public HAConnect getNextConnect() {
		// TODO Auto-generated method stub
		return null;
	}

	public WriteCache getWriteCache() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setInputSocket(ObjectSocketChannelStream in) {
		// TODO Auto-generated method stub
		
	}

	public void setNextOffset(long lastOffset) {
		// TODO Auto-generated method stub
		
	}

	public void truncate(long extent) {
		// TODO Auto-generated method stub
		
	}

}
