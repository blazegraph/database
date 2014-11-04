package com.bigdata.rdf.sail.webapp.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

import org.eclipse.jetty.client.AsyncContentProvider;
import org.eclipse.jetty.client.api.ContentProvider;
import org.eclipse.jetty.client.util.PathContentProvider;
import org.eclipse.jetty.util.B64Code;

import com.bigdata.btree.BytesUtil;

public class MultipartContentProvider implements AsyncContentProvider {
	
	final String m_boundary;
	
	final ByteArrayOutputStream m_data = new ByteArrayOutputStream();
	
	final Writer m_writer = new OutputStreamWriter(m_data);
	
	public MultipartContentProvider() {
		m_boundary = BytesUtil.toHexString(new int[] {new Random().nextInt()});
		
		try {
			m_writer.write("Content-Type: multipart/mixed; boundary=" + m_boundary + "\n");
		} catch (IOException e) {
			throw new RuntimeException("Unexpected", e);
		}
	}

	@Override
	public Iterator<ByteBuffer> iterator() {
		return new Iterator<ByteBuffer>() {
			ByteBuffer bb = ByteBuffer.wrap(m_data.toByteArray());
			
			@Override
			public boolean hasNext() {
				return bb != null;
			}

			@Override
			public ByteBuffer next() {
				if (bb == null) {
					throw new NoSuchElementException();
				}
				final ByteBuffer ret = bb;
				bb = null;
				
				return ret;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}

	@Override
	public long getLength() {
		return m_data.size();
	}
	
	/*
	 *    --AaB03x
   	 *	Content-Disposition: form-data; name="files"; filename="file1.txt"
   	 *	Content-Type: text/plain
	 */
	public void addPart(final String name, final ContentProvider part, final String mimetype) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setListener(Listener arg0) {
		throw new UnsupportedOperationException("Async support not yet available");
	}

	public void addPart(String name, byte[] data, String mimetype) {
		try {
			m_writer.write("\n--" + m_boundary + "\n");
			m_writer.write("Content-Disposition: form-data;");
			m_writer.write(" name=\"" + name + "\"\n");
			m_writer.write("Content-Type: " + mimetype +"\n");
			m_writer.write("Content-Transfer-Encoding: base64\n\n");
			
			m_writer.write(B64Code.encode(data));
			m_writer.write("\n");
			
			m_writer.flush();
		} catch (IOException e) {
			throw new RuntimeException("Unexpected", e);
		}
	}

}
