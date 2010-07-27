package benchmark.model;


public abstract class BSBMResource {
	Integer publisher=null;//Nr. of publisher
	Long publishDate=null;
	
	public Integer getPublisher() {
		return publisher;
	}
	public void setPublisher(Integer publisher) {
		this.publisher = publisher;
	}
	public Long getPublishDate() {
		return publishDate;
	}
	public void setPublishDate(Long publishDate) {
		this.publishDate = publishDate;
	}
}
