package br.com.starcode.threadswithtransactionexample;
import java.util.Properties;

import javax.sql.DataSource;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import com.atomikos.icatch.jta.UserTransactionManager;
import com.atomikos.jdbc.AtomikosDataSourceBean;

public class AtomikosDataSource {

	// Atomikos implementations
	private static UserTransactionManager utm;
	private static AtomikosDataSourceBean adsb;

	// Standard interfaces
	//private static TransactionManager tm;
	//private static DataSource ds;
	
	/**
	 * Mudar o valor de "max_prepared_transactions" no arquivo de configuração "postgresql.conf" do PostgreSQL
	 */

	// initialize resources
	public static void init() {
		//System.setProperty("com.atomikos.icatch.threaded_2pc", "true");
		utm = new UserTransactionManager();
		try {
			utm.init();
			adsb = new AtomikosDataSourceBean();
			adsb.setMaxPoolSize(20);
			adsb.setUniqueResourceName("postgres");
			adsb.setXaDataSourceClassName("org.postgresql.xa.PGXADataSource");
			Properties p = new Properties();
			p.setProperty("user", "postgres");
			p.setProperty("password", "0");
			p.setProperty("serverName", "localhost");
			p.setProperty("portNumber", "5432");
			p.setProperty("databaseName", "postgres");
			adsb.setXaProperties(p);
			
			//adsb.getXaDataSource().getXAConnection().getXAResource();
			//ds.getConnection().get
			//tm.getTransaction().

		} catch (SystemException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}
	
	// release resources
	public static void shutdown() {
		adsb.close();
		utm.close();
	}
	
	public static AtomikosDataSourceBean getDS() {
		return adsb;
	}
	
	public static TransactionManager getTM() {
		return utm;
	}
	
}