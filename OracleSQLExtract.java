import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;

import oracle.jdbc.OracleResultSet;
import oracle.sql.CLOB;

public class OracleSQLExtract {

	static String sSrcUrl = "";
	static String sSrcDriver = "";
	static String sSrcDbId = "";
	static String sSrcDbPw = "";
	static String sChkJobSql = "";
	
	static String sSessionCheckSql = "";
	static String sSqlFullTextSql = "";
	static String sSqlBindValueSql = "";
	static String sSqlExistsCheckSql = "";
	static String sSqlInstSql = "";
	static String sSqlUpdtClobSql = "";
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		System.out.println("Oracle SQL Explain Compare Start!!!");
		
		FileToProps(args[0]);
		
		/*
		System.out.println("Session Check SQL : " + sSessionCheckSql);
		System.out.println("Full Text Check SQL : " + sSqlFullTextSql);
		System.out.println("Bind Value Check SQL : " + sSqlBindValueSql);
		System.out.println("SQLID Exists Check SQL : " + sSqlExistsCheckSql);
		System.out.println("SQLID Insert SQL : " + sSqlInstSql);
		System.out.println("Update Clob SQL : " + sSqlUpdtClobSql);
		*/
		
		Connection dbConn = null;
		ResultSet session_rslt = null;
		
		PreparedStatement pSqlFull = null;
		ResultSet sqlfulltext_rslt = null;
		
		PreparedStatement pSqlExists = null;
		ResultSet query_exists_cnt = null;
		
		PreparedStatement pBindValue = null;
		ResultSet bindvalue_rslt = null;
		
		PreparedStatement pSqlInst = null;
		
		PreparedStatement pSqlUpdtClob = null;
		ResultSet clobRslt = null;
		
		BufferedWriter sbWriter = null;
		
		try {
			
			Class.forName(sSrcDriver);
			dbConn = DriverManager.getConnection(sSrcUrl, sSrcDbId, sSrcDbPw);
			
			session_rslt = dbConn.prepareStatement(sSessionCheckSql).executeQuery(); // session check 
			
			String sSqlId = "";
			
			while(session_rslt.next()) {
				
				sSqlId = session_rslt.getString("SQL_ID");
				
				pSqlFull = dbConn.prepareStatement(sSqlFullTextSql); // get full text in session check by sqlid
				pSqlFull.setString(1, sSqlId);
				sqlfulltext_rslt = pSqlFull.executeQuery();
				
					String sChkDt = "";
					String sChildAddress = "";
					String sSqlFullText = "";
								
					while(sqlfulltext_rslt.next()) {
						
						// after session check, get sql info : start
						sChkDt = sqlfulltext_rslt.getString("CHK_DT");
						sChildAddress = sqlfulltext_rslt.getString("CHILD_ADDRESS");
						
						// Get Full_Text SQL
						Reader clobStrm = null;
						
						StringBuffer sb = new StringBuffer();
						
						String sFullTextSqlRslt = "";
							
						clobStrm = sqlfulltext_rslt.getCharacterStream("SQL_FULLTEXT");
					    
					    char[] tmpBuffer = new char[1024];
					    
					    int length = 0;
					    
					    while ((length = clobStrm.read(tmpBuffer)) != -1)  {

					        for (int i = 0; i < length; i++){

					            sb.append(tmpBuffer[i]);

					        }

					    }
					    
					    sFullTextSqlRslt = sb.toString();
						// after session check, get sql info : end
						
						// check exists session check info : start
						pSqlExists = dbConn.prepareStatement(sSqlExistsCheckSql);
						pSqlExists.setString(1, sChkDt);
						pSqlExists.setString(2, sSqlId);
						pSqlExists.setString(3, sChildAddress);
						
						query_exists_cnt = pSqlExists.executeQuery();
						
						int iExistCnt = 0;
						
						while (query_exists_cnt.next()) {
							
							iExistCnt = query_exists_cnt.getInt("EXISTS_CNT");
							
							break;
						}
						// check exists session check info : end
						
						
						// if not exists, insert info : start
						if ( iExistCnt > 0 ) {
							System.out.println("SQL_ID, CHILD_ADDRESS Is Exists In SQL_EXPLN_CHK Table!!!");
						} else {
							
							// get bind value info : start
							pBindValue = dbConn.prepareStatement(sSqlBindValueSql);
							pBindValue.setString(1, sSqlId);
							pBindValue.setString(2, sChildAddress);
							
							bindvalue_rslt = pBindValue.executeQuery();
							
							String sBindValue = "";
							
							while (bindvalue_rslt.next()) {
								
								sBindValue = bindvalue_rslt.getString("BIND_VALUE");
								
								break;
							}
							//get bind value info : end
							
							pSqlInst = dbConn.prepareStatement(sSqlInstSql);
							pSqlInst.setString(1, sChkDt);
							pSqlInst.setString(2, sSqlId);
							pSqlInst.setString(3, sChildAddress);
							
							if (sBindValue == null) {
								pSqlInst.setString(4, "BIND VALUE NOT EXISTS");
							} else {
								pSqlInst.setString(4, sBindValue);
							}
							
							pSqlInst.execute();
							dbConn.setAutoCommit(false);
							
							pSqlUpdtClob = dbConn.prepareStatement(sSqlUpdtClobSql);
							pSqlUpdtClob.setString(1, sChkDt);
							pSqlUpdtClob.setString(2, sSqlId);
							pSqlUpdtClob.setString(3, sChildAddress);
							
							clobRslt = pSqlUpdtClob.executeQuery();

							if(clobRslt.next()) {
								
								CLOB clobStr = ((OracleResultSet)clobRslt).getCLOB("SQL_FULLTEXT");
								sbWriter = new BufferedWriter(clobStr.getCharacterOutputStream());
								sbWriter.write(sb.toString());
								sbWriter.close();
							}
							
							dbConn.commit();
							dbConn.setAutoCommit(true);

						}
						// if not exists, insert info : end						
						
					}
			}
			
		} catch (Exception e) {
			System.out.println("DB Exception : " + e.getMessage());
			e.printStackTrace();
		} finally {
			
			try {
				if(sbWriter != null) {
					sbWriter.close();
				}
				
				if(clobRslt != null) {
					clobRslt.close();
				}
				
				if(pSqlUpdtClob != null) {
					pSqlUpdtClob.close();
				}
				
				if(pSqlInst != null) {
					pSqlInst.close();
				}
				
				if(bindvalue_rslt != null) {
					bindvalue_rslt.close();
				}
				
				if(pBindValue != null) {
					pBindValue.close();
				}
				
				if(query_exists_cnt != null) {
					query_exists_cnt.close();
				}
				
				if(pSqlExists != null) {
					pSqlExists.close();
				}
				
				if(sqlfulltext_rslt != null) {
					sqlfulltext_rslt.close();
				}
				
				if(pSqlFull != null) {
					pSqlFull.close();
				}
				
				if(session_rslt != null) {
					session_rslt.close();
				}
				
				if(dbConn != null) {
					dbConn.close();
				}
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		
	}
	
	public static void FileToProps(String sFile) {

		Properties prop = new Properties();
		InputStream is = null;

		try {
			is = new FileInputStream(sFile);
			prop.load(is);

			sSrcUrl = prop.getProperty("SRC_JDBC_URL");
			sSrcDriver = prop.getProperty("SRC_JDBC_DRIVER");
			sSrcDbId = prop.getProperty("SRC_DB_ID");
			sSrcDbPw = prop.getProperty("SRC_DB_PW");
			
			sChkJobSql = prop.getProperty("CHECK_JOB_SQL");
		} catch (Exception e) {
			System.out.println("FileToProps Exception : " + e.getMessage());
		}
		
		Document document = null;
		Properties pJobQuery = null;

		try {
			document = new SAXBuilder().build(new FileInputStream(sChkJobSql));
			pJobQuery = new Properties();

			Element element = document.getRootElement();
			List childElementList = element.getChildren();

			for (int i = 0; i < childElementList.size(); i++) {
				pJobQuery.setProperty(((Element) childElementList.get(i)).getName(),
						((Element) childElementList.get(i)).getValue());
			}

			HashMap<String, String> mJobQueryMap = new HashMap<String, String>((Map)pJobQuery);
			
			Set sQuerySet = mJobQueryMap.entrySet();;
			
			for (Object obj : sQuerySet) {
			
				Map.Entry queryEntry = (Map.Entry) obj;
				
				if (queryEntry.getKey().toString().trim().equals("ORACLE_SESSION_CHECK")) {
					sSessionCheckSql = queryEntry.getValue().toString().trim();
				} else if (queryEntry.getKey().toString().trim().equals("ORACLE_SQL_FULLTEXT")) {
					sSqlFullTextSql = queryEntry.getValue().toString().trim();
				} else if (queryEntry.getKey().toString().trim().equals("ORACLE_SQL_BIND_VALUE")) {
					sSqlBindValueSql = queryEntry.getValue().toString().trim();
				} else if (queryEntry.getKey().toString().trim().equals("ORACLE_EXISTS_CHECK")) {
					sSqlExistsCheckSql = queryEntry.getValue().toString().trim();
				} else if (queryEntry.getKey().toString().trim().equals("ORACLE_INS_SQL")) {
					sSqlInstSql = queryEntry.getValue().toString().trim();
				} else if (queryEntry.getKey().toString().trim().equals("ORACLE_UPD_CLOB_SQL")) {
					sSqlUpdtClobSql = queryEntry.getValue().toString().trim();
				}  
			}
			
		} catch (Exception e) {
			System.out.println("Get Check Job SQL Load Exception : " + e.getMessage());
		}
	}

}
