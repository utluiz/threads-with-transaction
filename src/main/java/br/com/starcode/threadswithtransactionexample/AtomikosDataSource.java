package br.com.starcode.threadswithtransactionexample;
import java.util.Properties;

import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import com.atomikos.icatch.jta.UserTransactionManager;
import com.atomikos.jdbc.AtomikosDataSourceBean;

/**
 * TODO FIXME It's necessary set "max_prepared_transactions" 
 * with a value greater than zero in the PostgreSQL's configuration 
 * file "postgresql.conf"
 */
public class AtomikosDataSource {

	// Atomikos implementations
	private static UserTransactionManager utm;
	private static AtomikosDataSourceBean adsb;

	// initialize resources
	public static void init() {
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
	
	// get datasource
	public static AtomikosDataSourceBean getDS() {
		return adsb;
	}
	
	// get transaction manager
	public static TransactionManager getTM() {
		return utm;
	}
	
}