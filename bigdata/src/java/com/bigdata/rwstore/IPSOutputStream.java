package com.bigdata.rwstore;

import java.io.OutputStream;

public abstract class IPSOutputStream extends OutputStream {

	public abstract long getAddr();
}
