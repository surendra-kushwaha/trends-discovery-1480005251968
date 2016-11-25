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

		try {
			if ((connection == null) || connection.isClosed()) {
				connection = DataBase.getInstance().getConnection();
			}

			ps = connection.prepareStatement(SQL_INSERT_SEMRUSH_METAKEYWORD);
			ps.setString(1, metaKeywords.getKeywordName());
			ps.setDate(2, new Date(metaKeywords.getLastUpdateDttm().getTime()));
			ps.setString(3, metaKeywords.getActive());
			ps.setString(4, metaKeywords.getModifiedBy());

			ps.execute();

		} catch (Exception e) {

			log.log(Level.SEVERE, "error while addSemurshMetaKeyword" + e.getMessage(), e);
			e.printStackTrace();
			return false;

		} finally {
			close(rs, ps, connection);
		}

		return true;
	}

	/***
	 * Add twitter meta keyword
	 * @param metaKeywords
	 * @return
	 */
	public boolean addTwitterMetaKeyword(MetaKeywords metaKeywords) {

		log.info("In MetaKeywordsDAO - addTwitterMetaKeyword >>>");
		log.info("SQL_INSERT_TWITTER_METAKEYWORD : "+SQL_INSERT_TWITTER_METAKEYWORD );
		try {
			if ((connection == null) || connection.isClosed()) {
				connection = DataBase.getInstance().getConnection();
				log.info("DB Connection " + connection);
			}

			
			ps = connection.prepareStatement(SQL_INSERT_TWITTER_METAKEYWORD);
			ps.setString(1, metaKeywords.getKeywordName());
			ps.setDate(2, new Date(metaKeywords.getLastUpdateDttm().getTime()));
			ps.setString(3, metaKeywords.getActive());
			ps.setString(4, metaKeywords.getModifiedBy());

			ps.execute();

		} catch (Exception e) {
			log.log(Level.SEVERE, "error while addTwitterMetaKeyword" + e.getMessage(), e);
			e.printStackTrace();
			return false;
		} finally {
			close(rs, ps, connection);
		}

		return true;
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