package benchmark.serializer;

import java.util.*;
import benchmark.model.*;

public class ObjectBundle {
	private String graphName;
	private int publisherNum;
	private String publisher;
	private long publishDate;
	private Vector<BSBMResource> objects;
	private Serializer serializer;
	private int maxSize;
	private int size;
	private boolean finish;
	
	public boolean isFinish() {
		return finish;
	}

	public void setFinish(boolean finish) {
		this.finish = finish;
	}

	public ObjectBundle(Serializer serializer)
	{
		objects = new Vector<BSBMResource>();
		maxSize = 0;
		size = 0;
		this.serializer = serializer;
		finish = false;
	}
	
	public ObjectBundle(Serializer serializer, int maxsize)
	{
		objects = new Vector<BSBMResource>(maxsize);
		maxSize = maxsize;
		size = 0;
		this.serializer = serializer;
		finish = false;
	}
	
	public void add(BSBMResource res)
	{
		objects.add(res);
		
		//Only if maxSize is set, automatic commit active
		if(maxSize>0)
		{
			size++;
			if(size==maxSize)
				commitToSerializer();
		}
	}
	
	public int size()
	{
		return objects.size();
	}

	public String getGraphName() {
		return graphName;
	}
	
	public boolean commitToSerializer()
	{
		//Only do this if Serializer is set
		if(serializer!=null) {
			serializer.gatherData(this);
			size=0;
			objects = new Vector<BSBMResource>(maxSize);
			return true;
		}else
			return false;
	}

	public void setGraphName(String namedGraph) {
		this.graphName = namedGraph;
	}
	
	public Iterator<BSBMResource> iterator()
	{
		return objects.iterator();
	}


	public String getPublisher() {
		return publisher;
	}

	public void setPublisher(String publisher) {
		this.publisher = publisher;
	}

	public long getPublishDate() {
		return publishDate;
	}

	public void setPublishDate(long publishDate) {
		this.publishDate = publishDate;
	}

	public int getPublisherNum() {
		return publisherNum;
	}

	public void setPublisherNum(int publisherNum) {
		this.publisherNum = publisherNum;
	}
	
	public boolean writeStringToSerializer(String s) {
		if(serializer instanceof NTriples) {
			((NTriples)serializer).writeString(s);
			return true;
		}
		else
			return false;
	}
}
