/**
 * 
 */
package com.acit.trendsdiscovery.model;

import java.util.Date;

/**
 * Meta keywords model
 *
 */
public class MetaKeywords {
	
	private int keywordID;
	private String keywordName;
	private Date lastUpdateDttm;
	private String active;
	private String modifiedBy;
	
	
	public int getKeywordID() {
		return keywordID;
	}
	public void setKeywordID(int keywordID) {
		this.keywordID = keywordID;
	}
	public String getKeywordName() {
		return keywordName;
	}
	public void setKeywordName(String keywordName) {
		this.keywordName = keywordName;
	}
	public Date getLastUpdateDttm() {
		return lastUpdateDttm;
	}
	public void setLastUpdateDttm(Date lastUpdateDttm) {
		this.lastUpdateDttm = lastUpdateDttm;
	}
	public String getActive() {
		return active;
	}
	public void setActive(String active) {
		this.active = active;
	}
	public String getModifiedBy() {
		return modifiedBy;
	}
	public void setModifiedBy(String modifiedBy) {
		this.modifiedBy = modifiedBy;
	}
	
	
	

}
