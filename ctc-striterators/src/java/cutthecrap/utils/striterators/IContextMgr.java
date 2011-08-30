package cutthecrap.utils.striterators;

import java.util.Iterator;

public interface IContextMgr {

	void pushContext(Object ocntext);
	
	void popContext();

}
