package domain;

import java.util.List;

public class Article2 {

	private Integer id;
	private String title;

	private List<Commentator> commentators;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public List<Commentator> getCommentators() {
		return commentators;
	}

	public void setCommentators(List<Commentator> commentators) {
		this.commentators = commentators;
	}

}
