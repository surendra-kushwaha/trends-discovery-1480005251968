package com.acit.trendsdiscovery.model;

/***
 * 
 * Trend Info Model
 *
 */
public class TrendInfo {
	String trendUnit;
	String trendText;
	String topicName;
	String trendSource;
	String trendValue;
	int trendTopicId;
	String trendUpdationDate;
	
	
	public String getTrendUpdationDate() {
		return trendUpdationDate;
	}
	public void setTrendUpdationDate(String trendUpdationDate) {
		this.trendUpdationDate = trendUpdationDate;
	}
	
	public String getTopicName() {
		return topicName;
	}
	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
	public String getTrendUnit() {
		return trendUnit;
	}
	public void setTrendUnit(String trendUnit) {
		this.trendUnit = trendUnit;
	}
	public String getTrendText() {
		return trendText;
	}
	public void setTrendText(String trendText) {
		this.trendText = trendText;
	}
	public String getTrendSource() {
		return trendSource;
	}
	public void setTrendSource(String trendSource) {
		this.trendSource = trendSource;
	}
	public String getTrendValue() {
		return trendValue;
	}
	public void setTrendValue(String trendValue) {
		this.trendValue = trendValue;
	}
	public int getTrendTopicId() {
		return trendTopicId;
	}
	public void setTrendTopicId(int trendTopicId) {
		this.trendTopicId = trendTopicId;
	}
	
	
}
