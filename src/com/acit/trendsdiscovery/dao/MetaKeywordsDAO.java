/**
 * 
 */
package com.acit.trendsdiscovery.dao;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.acit.trendsdiscovery.model.MetaKeywords;
import com.acit.trendsdiscovery.util.DataBase;

/**
 * @author indira
 *
 */
public class MetaKeywordsDAO {

	private static final Logger log = Logger.getLogger(MetaKeywordsDAO.class.getName());

	private static String SQL_GET_ID_SEMRUSH_METAKEYWORD ="SELECT KEYWORD_ID FROM SEMRUSH_META_KEYWORDS WHERE LOWER(KEYWORD_NAME)=?";
	// System.getenv("SQL_GET_ID_SEMRUSH_METAKEYWORD");
	private static String SQL_GET_ID_TWITTER_METAKEYWORD ="SELECT KEYWORD_ID FROM TWITTER_META_KEYWORDS WHERE LOWER(KEYWORD_NAME)=?"; 
	//System.getenv("SQL_GET_ID_TWITTER_METAKEYWORD");
	
	
			

	private static String SQLGET_SEMRUSH_METAKEYWORD ="SELECT * FROM SEMRUSH_META_KEYWORDS WHERE KEYWORD_NAME = ?"; 
	//System.getenv("SQLGET_SEMRUSH_METAKEYWORD");
	
	private static String SQL_INSERT_SEMRUSH_METAKEYWORD = System.getenv("SQL_INSERT_SEMRUSH_METAKEYWORD");
	private static String SQL_INSERT_TWITTER_METAKEYWORD = System.getenv("SQL_INSERT_TWITTER_METAKEYWORD");
	
	//TO-DO:
	private static String SQL_UPDATE_SEMRUSH_METAKEYWORD = "UPDATE SEMRUSH_META_KEYWORDS SET ACTIVE=? WHERE  LOWER(KEYWORD_NAME)=?";
	//System.getenv("SQL_UPDATE_SEMRUSH_METAKEYWORD");
	private static String SQL_UPDATE_TWITTER_METAKEYWORD = "UPDATE SEMRUSH_META_KEYWORDS SET ACTIVE=? WHERE  LOWER(KEYWORD_NAME)=?";
	//System.getenv("SQL_UPDATE_TWITTER_METAKEYWORD");
	
	
//	private static String SQL_GET_ID_SEMRUSH_METAKEYWORD = "SELECT KEYWORD_ID FROM SEMRUSH_META_KEYWORDS "
//			+ "WHERE KEYWORD_NAME=?";
//	
//	private static String SQL_GET_ID_TWITTER_METAKEYWORD = "SELECT KEYWORD_ID FROM TWITTER_META_KEYWORDS "
//			+ "WHERE KEYWORD_NAME=?";
//
//	private static String SQLGET_SEMRUSH_METAKEYWORD = "SELECT * FROM SEMRUSH_META_KEYWORDS "
//			+ " WHERE KEYWORD_NAME = ?";
//
//	private static String SQL_INSERT_SEMRUSH_METAKEYWORD = "INSERT INTO SEMRUSH_META_KEYWORDS("
//			+ "KEYWORD_NAME,LAST_UPDATE_DTTM,ACTIVE,MODIFIED_BY) " + "VALUES(?,?,?,?)";
//
//	private static String SQL_INSERT_TWITTER_METAKEYWORD = "INSERT INTO TWITTER_META_KEYWORDS("
//			+ "KEYWORD_NAME,LAST_UPDATE_DTTM,ACTIVE,MODIFIED_BY) " + "VALUES(?,?,?,?)";


	private Connection connection;
	PreparedStatement ps = null;
	ResultSet rs = null;

	/***
	 * Connection
	 */
	public MetaKeywordsDAO() {
		connection = DataBase.getInstance().getConnection();
	}

	/***
	 * Retrieve ID for the Semrush meta keyword
	 * @param keyword
	 * @return
	 */
	public int getIdOfSemrushMetaKeyword(String keyword) {

		log.info("In MetaKeywordsDAO - getIdOfSemrushMetaKeyword >>> "+keyword);
		log.info("SQL_GET_ID_SEMRUSH_METAKEYWORD : "+SQL_GET_ID_SEMRUSH_METAKEYWORD );
		int keyWordID = 0;

		try {
			if ((connection == null) || connection.isClosed()) {
				connection = DataBase.getInstance().getConnection();
			}

			ps = connection.prepareStatement(SQL_GET_ID_SEMRUSH_METAKEYWORD);
			ps.setString(1, keyword.toLowerCase());
			rs = ps.executeQuery();
			
			while (rs.next()) {
				keyWordID = rs.getInt("KEYWORD_ID");
				log.info("Keyword exists ID : " + keyWordID);
				return keyWordID;
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "error while getIdOfSemrushMetaKeyword" + e.getMessage(), e);
			e.printStackTrace();
		} finally {
			close(rs, ps, connection);
		}
		return keyWordID;

	}
	
	/***
	 * Retrieve Id for the twitter meta keyword
	 * @param keyword
	 * @return
	 */
	public int getIdOfTwitterMetaKeyword(String keyword) {

		log.info("In MetaKeywordsDAO - getIdOfTwitterMetaKeyword >>>");
		log.info("SQL_GET_ID_TWITTER_METAKEYWORD : "+SQL_GET_ID_TWITTER_METAKEYWORD );

		int keyWordID = 0;

		try {
			if ((connection == null) || connection.isClosed()) {
				connection = DataBase.getInstance().getConnection();
			}

			ps = connection.prepareStatement(SQL_GET_ID_TWITTER_METAKEYWORD);
			ps.setString(1, keyword.toLowerCase());
			rs = ps.executeQuery();

			while (rs.next()) {
				keyWordID = rs.getInt("KEYWORD_ID");
				log.info("Keyword exists ID : " + keyWordID);

				return keyWordID;
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "error while getIdOfTwitterMetaKeyword" + e.getMessage(), e);
		} finally {
			close(rs, ps, connection);
		}
		return keyWordID;

	}

	/***
	 * Retrieve list of semrush meta keywords
	 * @return
	 */
	public List<MetaKeywords> getSemrushMetaKeywords(String keyword) {

		log.info("In MetaKeywordsDAO - getSemrushMetaKeywords >>>");
		log.info("SQLGET_SEMRUSH_METAKEYWORD : "+SQLGET_SEMRUSH_METAKEYWORD );

		List<MetaKeywords> metaKeywords = new ArrayList<MetaKeywords>();

		try {
			if ((connection == null) || connection.isClosed()) {
				connection = DataBase.getInstance().getConnection();
			}

			ps = connection.prepareStatement(SQLGET_SEMRUSH_METAKEYWORD);
			ps.setString(1, keyword.toLowerCase());
			rs = ps.executeQuery();

			while (rs.next()) {

				MetaKeywords meKeyword = new MetaKeywords();
				meKeyword.setKeywordID(rs.getInt("KEYWORD_ID"));
				meKeyword.setKeywordName(rs.getString("KEYWORD_NAME"));
				meKeyword.setLastUpdateDttm(rs.getDate("LAST_UPDATE_DTTM"));
				meKeyword.setActive(rs.getString("ACTIVE"));
				meKeyword.setModifiedBy(rs.getString("MODIFIED_BY"));

				metaKeywords.add(meKeyword);

			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "error while getSemrushMetaKeywords" + e.getMessage(), e);
		} finally {
			close(rs, ps, connection);
		}
		return metaKeywords;

	}

	/***
	 * Add semrush meta keyword
	 * @param metaKeywords
	 * @return
	 */
	public boolean addSemurshMetaKeyword(MetaKeywords metaKeywords) {

		log.info("In MetaKeywordsDAO - addSemurshMetaKeyword >>>");
		log.info("SQL_INSERT_SEMRUSH_METAKEYWORD : "+SQL_INSERT_SEMRUSH_METAKEYWORD );
		boolean updateSuccessFlag = false;
		try {
			if ((connection == null) || connection.isClosed()) {
				connection = DataBase.getInstance().getConnection();
			}
			
			System.out.println("metaKeywords.getKeywordName()::"+metaKeywords.getKeywordName());
			
			if (!validateSemrushKeyword(metaKeywords.getKeywordName())) {
				System.out.println("keyword does not exists in semrush master keyword");
				ps = connection.prepareStatement(SQL_INSERT_SEMRUSH_METAKEYWORD);
				ps.setString(1, metaKeywords.getKeywordName());
				ps.setDate(2, new Date(metaKeywords.getLastUpdateDttm().getTime()));
				ps.setString(3, metaKeywords.getActive());
				ps.setString(4, metaKeywords.getModifiedBy());
	
				int updateflag = ps.executeUpdate();
				if (updateflag > 0) {
					updateSuccessFlag = true;
				}
			}

		} catch (Exception e) {

			log.log(Level.SEVERE, "error while addSemurshMetaKeyword" + e.getMessage(), e);
			e.printStackTrace();
			return false;

		} finally {
			close(rs, ps, connection);
		}

		return updateSuccessFlag;
	}

	/***
	 * Add twitter meta keyword
	 * @param metaKeywords
	 * @return
	 */
	public boolean addTwitterMetaKeyword(MetaKeywords metaKeywords) {

		log.info("In MetaKeywordsDAO - addTwitterMetaKeyword >>>");
		log.info("SQL_INSERT_TWITTER_METAKEYWORD : "+SQL_INSERT_TWITTER_METAKEYWORD );
		boolean updateSuccessFlag = false;
		try {
			if ((connection == null) || connection.isClosed()) {
				connection = DataBase.getInstance().getConnection();
				log.info("DB Connection " + connection);
			}

			if (!validateTwitterKeyword(metaKeywords.getKeywordName())) {
				System.out.println("keyword does not exists in semrush master keyword");
				ps = connection.prepareStatement(SQL_INSERT_TWITTER_METAKEYWORD);
				ps.setString(1, metaKeywords.getKeywordName());
				ps.setDate(2, new Date(metaKeywords.getLastUpdateDttm().getTime()));
				ps.setString(3, metaKeywords.getActive());
				ps.setString(4, metaKeywords.getModifiedBy());
	
				int updateflag = ps.executeUpdate();
				if (updateflag > 0) {
					updateSuccessFlag = true;
				}
			}

		} catch (Exception e) {
			log.log(Level.SEVERE, "error while addTwitterMetaKeyword" + e.getMessage(), e);
			e.printStackTrace();
			return false;
		} finally {
			close(rs, ps, connection);
		}

		return updateSuccessFlag;
	}
	
	/****
	 * Check if the topic exists already
	 * 
	 * @param topicName
	 * @return
	 */
	public boolean validateSemrushKeyword(String topicName) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean topicAvailable = false;
		//List<Integer> topicIdList = new ArrayList<Integer>();
		System.out.println("semrush trends name::"+topicName);
		try {
			if ((connection == null) || connection.isClosed()) {
				connection = DataBase.getInstance().getConnection();
			}
			ps = connection.prepareStatement("select * from semrush_meta_keywords where keyword_name=?");
			ps.setString(1, topicName);
			rs = ps.executeQuery();
			while (rs.next()) {
				System.out.println("topic name present in semrush");
				//topicIdList.add(rs.getInt("TOPIC_ID"));
				topicAvailable = true;
			}
		} catch (Exception ex) {
			System.out.println("Error in check() -->" + ex.getMessage());
		} finally {
			close(rs, ps, null);
		}
		//System.out.println("topicIdList size##" + topicIdList);
		return topicAvailable;
	}
	
	/****
	 * Check if the topic exists already
	 * 
	 * @param topicName
	 * @return
	 */
	public boolean validateTwitterKeyword(String topicName) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean topicAvailable = false;
		//List<Integer> topicIdList = new ArrayList<Integer>();

		try {
			if ((connection == null) || connection.isClosed()) {
				connection = DataBase.getInstance().getConnection();
			}
			ps = connection.prepareStatement("select * from twitter_meta_keywords where keyword_name=?");
			ps.setString(1, topicName);
			rs = ps.executeQuery();
			while (rs.next()) {
				//topicIdList.add(rs.getInt("TOPIC_ID"));
				topicAvailable = true;
			}
		} catch (Exception ex) {
			System.out.println("Error in check() -->" + ex.getMessage());
		} finally {
			close(rs, ps, null);
		}
		//System.out.println("topicIdList size##" + topicIdList);
		return topicAvailable;
	}
	
	/***
	 * Retrieve list of trends discovery keywords
	 * @return
	 */
	public List<String> getTrendsDiscoveryData() {
		log.info("In MetaKeywordsDAO - getTrendsDiscoveryData >>>");
		//log.info("SQLGET_SEMRUSH_METAKEYWORD : "+SQLGET_SEMRUSH_METAKEYWORD );
		List<String> trendsData = new ArrayList<String>();
		try {
			if ((connection == null) || connection.isClosed()) {
				connection = DataBase.getInstance().getConnection();
			}
			//introduce 2 new column int id and processed_flag. For every 120 records put flag as Y
			String sqlString="select * from TRENDS_DISCOVERY_INPUTS where is_processed_flag='N'";			
			ps = connection.prepareStatement(sqlString);
			//ps.setString(1, keyword.toLowerCase());
			rs = ps.executeQuery();
			int id=0;
			while (rs.next()) {
				id=rs.getInt("MIK_SUBCLASS_ID");									
			}
			
			int idNew=id+120;
			String sqlstr="select * from TRENDS_DISCOVERY_INPUTS where mik_subclass_id>"+id+" and mik_subclass_id<"+idNew+"";				
			System.out.println(sqlstr);
			ps = connection.prepareStatement(sqlstr);
			rs = ps.executeQuery();
			while (rs.next()) {
				trendsData.add(rs.getString("MIK_SUBCLASS_NAME"));
			}
						
			String sqlstr1="update TRENDS_DISCOVERY_INPUTS set is_processed_flag='N' where mik_subclass_id="+idNew+"";	
			//System.out.println(sqlstr1);
			ps = connection.prepareStatement(sqlstr1);
			ps.executeUpdate();
			
			String sqlstr2="update TRENDS_DISCOVERY_INPUTS set is_processed_flag='NA' where mik_subclass_id="+id+"";	
			//System.out.println(sqlstr2);
			ps = connection.prepareStatement(sqlstr2);
			ps.executeUpdate();
		} catch (Exception e) {
			log.log(Level.SEVERE, "error while getSemrushMetaKeywords" + e.getMessage(), e);
		} finally {
			close(rs, ps, connection);
		}
		return trendsData;
	}

	/***
	 * Retrieve list of trends discovery keywords
	 * @return
	 */
	public List<String> getDepartments() {
		log.info("In MetaKeywordsDAO - getTrendsDiscoveryData >>>");
		//log.info("SQLGET_SEMRUSH_METAKEYWORD : "+SQLGET_SEMRUSH_METAKEYWORD );
		List<String> departmentList = new ArrayList<String>();
		try {
			if ((connection == null) || connection.isClosed()) {
				connection = DataBase.getInstance().getConnection();
			}
			//introduce 2 new column int id and processed_flag. For every 120 records put flag as Y
			String sqlString="select * from MICHAELS_DEPARTMENTS";			
			ps = connection.prepareStatement(sqlString);
			//ps.setString(1, keyword.toLowerCase());
			rs = ps.executeQuery();
			//int id=0;
			while (rs.next()) {
				departmentList.add(rs.getString("MIK_DEPT_NAME"));									
			}
		} catch (Exception e) {
			log.log(Level.SEVERE, "error while getSemrushMetaKeywords" + e.getMessage(), e);
		} finally {
			close(rs, ps, connection);
		}
		return departmentList;
	}

	//TOOD::
	
	public boolean updateSemrushMetaKeywordStatus(MetaKeywords metaKeywords) {

		log.info("In MetaKeywordsDAO - updateSemrushMetaKeywordStatus >>>");
		log.info("SQL_UPDATE_SEMRUSH_METAKEYWORD : "+SQL_UPDATE_SEMRUSH_METAKEYWORD );
		try {
			if ((connection == null) || connection.isClosed()) {
				connection = DataBase.getInstance().getConnection();
				log.info("DB Connection " + connection);
			}
			
			ps = connection.prepareStatement(SQL_UPDATE_SEMRUSH_METAKEYWORD);
			
			ps.setString(1, metaKeywords.getActive());
			ps.setString(2, metaKeywords.getKeywordName().toLowerCase());

			ps.execute();

		} catch (Exception e) {
			log.log(Level.SEVERE, "error while updating status of semrush MetaKeyword " + e.getMessage(), e);
			e.printStackTrace();
			return false;
		} finally {
			close(rs, ps, connection);
		}

		return true;
	}
	public boolean updateTwitterMetaKeywordStatus(MetaKeywords metaKeywords) {

		log.info("In MetaKeywordsDAO - updateTwitterMetaKeywordStatus >>>");
		log.info("SQL_UPDATE_TWITTER_METAKEYWORD : "+SQL_UPDATE_TWITTER_METAKEYWORD );
		try {
			if ((connection == null) || connection.isClosed()) {
				connection = DataBase.getInstance().getConnection();
				log.info("DB Connection " + connection);
			}

			
			ps = connection.prepareStatement(SQL_UPDATE_TWITTER_METAKEYWORD);
			ps.setString(1, metaKeywords.getActive());
			ps.setString(2, metaKeywords.getKeywordName().toLowerCase());

			ps.execute();

		} catch (Exception e) {
			log.log(Level.SEVERE, "error while  updating status of twitter MetaKeyword" + e.getMessage(), e);
			e.printStackTrace();
			return false;
		} finally {
			close(rs, ps, connection);
		}

		return true;
	}
	
	/***
	 * Close
	 * @param rs
	 * @param pstmt
	 * @param conn
	 */
	public void close(ResultSet rs, PreparedStatement pstmt, Connection conn) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				log.info("The result set cannot be closed." + e);
			}
		}
		if (pstmt != null) {
			try {
				pstmt.close();
			} catch (SQLException e) {
				log.info("The prepared statement cannot be closed." + e);
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				log.info("The data source connection cannot be closed." + e);
			}
		}

	}

	public static void main(String[] args) {

		MetaKeywordsDAO metaKeywordsDAO = new MetaKeywordsDAO();
		// int keywordID = metaKeywordsDAO.getIdOfSemrushMetaKeyword("adult
		// coloring book");
		// log.info("ID is : "+keywordID);

		MetaKeywords metaKeywords = new MetaKeywords();
		metaKeywords.setActive("Y");
		metaKeywords.setKeywordName("TestKw1");
		metaKeywords.setLastUpdateDttm(new java.util.Date());
		metaKeywords.setModifiedBy("Sheethal");
//		metaKeywordsDAO.addSemurshMetaKeyword(metaKeywords);
//		metaKeywordsDAO.addTwitterMetaKeyword(metaKeywords);
		
		metaKeywordsDAO.getIdOfSemrushMetaKeyword("new laptops");
		//metaKeywordsDAO.getIdOfTwitterMetaKeyword("Silver");
		
	}

}
