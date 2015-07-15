package com.bigdata.io.writecache;

import com.bigdata.rwstore.RWStore;

public interface IBufferedWriter {

	int getSlotSize(int data_len);

	RWStore.StoreCounters<?> getStoreCounters();

}
