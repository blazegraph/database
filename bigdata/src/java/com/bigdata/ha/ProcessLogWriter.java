package com.bigdata.ha;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Formatter;

import com.bigdata.journal.ha.HAWriteMessage;

/**
 * Wrapper class to handle process log creation and output for HA.
 * 
 * The process log stores the HAWriteMessages and buffers to support reading and
 * reprocessing as part of the HA synchronization protocol.
 * 
 * The writer encapsulates not only the writing of individual messages but also
 * the closing and creation of new files.
 * 
 * @author Martyn Cutcher
 * 
 */
public class ProcessLogWriter {
	// current log file
	File m_log = null;
	
	// current log file commit counter
	long m_commitCounter = 0;
	
	// current output stream
	ObjectOutputStream m_out = null;
	
	// process log directory
	final File m_dir;

	public ProcessLogWriter(final File dir) {
		m_dir = dir;
	}
	
	/**
	 * 
	 * @param msg
	 * @param data
	 * @throws IOException
	 */
	public void write(final HAWriteMessage msg,
            final ByteBuffer data) throws IOException
	{
		if (msg.getSequence() == 0) { // new log required			
			cycleFile(msg);
		}
		
		if (m_out != null) {
			
			/*
			 * Check if this really is a valid message for this file.
			 * If it is not, then close the file and return immediately
			 */
			if (m_commitCounter != (msg.getCommitCounter()+1)) {
				close();
				
				return;
			}
			
			final byte[] array = data.array();

			m_out.writeObject(msg);
			assert msg.getSize() == array.length;

			m_out.write(data.array());
		}
	}

	private void cycleFile(final HAWriteMessage msg) throws FileNotFoundException, IOException {
		
		/*
		 * Close any open log
		 */
		close();
		
		/*
		 * The commit counter that will be used to identify the file.
		 * 
		 * Note: We use commitCounter+1 so the file will be labeled by the
		 * commit point that will be achieved when that log file is applied to a
		 * journal whose current commit point is [commitCounter].
		 */
		m_commitCounter = msg.getCommitCounter() + 1;

		/*
		 * Format the name of the log file.
		 * 
		 * Note: The commit counter in the file name should be zero filled to 20
		 * digits so we have the files in lexical order in the file system (for
		 * convenience).
		 */
		final String logFile;
		{

			final StringBuilder sb = new StringBuilder();

			final Formatter f = new Formatter(sb);

			f.format("%020d.log", m_commitCounter);

			logFile = sb.toString();

		}
		
		m_log = new File(m_dir, logFile);

		m_out = new ObjectOutputStream(new FileOutputStream(m_log));
	}
	
	/**
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		try {
			if (m_out != null) {
				m_out.close();			
			}
		} finally {
			m_out = null;
			m_log = null;
		}
	}
	
	/**
	 * When the HA leader commits it must flush the log
	 */
	public void flush() throws IOException {
		if (m_out != null) {
			m_out.flush();
		}
	}

	/**
	 * On various error conditions we may need to remove the log
	 * 
	 * @throws IOException
	 */
	public void remove() throws IOException {
		try {
			if (m_out != null) {
				m_out.close();
				m_log.delete();
			}
		} finally {
			m_out = null;
			m_log = null;
		}
	}
}
