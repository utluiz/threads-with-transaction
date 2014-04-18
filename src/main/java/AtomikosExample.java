import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.sql.DataSource;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import com.atomikos.icatch.jta.UserTransactionManager;
import com.atomikos.jdbc.AtomikosDataSourceBean;

public class AtomikosExample {

	// Atomikos implementations
	private UserTransactionManager utm;
	private AtomikosDataSourceBean adsb;

	// Standard interfaces
	private TransactionManager tm;
	private DataSource ds;

	// initialize resources
	private void init() throws SystemException {
		System.setProperty("com.atomikos.icatch.threaded_2pc", "true");
		utm = new UserTransactionManager();
		utm.init();
		tm = utm;
		adsb = new AtomikosDataSourceBean();
		adsb.setUniqueResourceName("postgres");
		adsb.setXaDataSourceClassName("org.postgresql.xa.PGXADataSource");
		Properties p = new Properties();
		p.setProperty("user", "postgres");
		p.setProperty("password", "0");
		p.setProperty("serverName", "localhost");
		p.setProperty("portNumber", "5432");
		p.setProperty("databaseName", "postgres");
		adsb.setXaProperties(p);

		ds = adsb;
	}

	// release resources
	private void shutdown() {
		adsb.close();
		utm.close();
	}
	
	private class Processamento implements Callable<Integer> {

		private int id;
		private boolean falhar;
		
		public Processamento(int id, boolean falhar) {
			this.falhar = falhar;
			this.id = id;
		}
		
		public Integer call() throws Exception {
			if (falhar) {
				throw new RuntimeException("Falhou inesperadamente!");
			}
			Connection c = ds.getConnection();
			Statement s = c.createStatement();
			s.executeUpdate("update teste set processado = '" + (falhar ? "falha" : "ok") + "' where id = " + id);
			c.close();
			return id;
		}
		
	}

	public boolean processar(boolean falhar) {
		try {
			init();
			tm.begin();

			ExecutorService executor = Executors.newFixedThreadPool(5);
			List<Callable<Integer>> processos = new ArrayList<Callable<Integer>>();
			for (int i = 0; i < 5; i++) {
				System.out.println(i);
				processos.add(new Processamento(i + 1, i == 4 && falhar));
			}
			List<Future<Integer>> futures = executor.invokeAll(processos);
			for (Future<Integer> future : futures) {
				System.out.println("Thread " + future.get() + " sucesso!");
			}

			tm.commit();
			System.out.println("Sucesso geral!");
			return true;
		} catch (Throwable e) {
			e.printStackTrace();
			try {
				tm.rollback();
			} catch (IllegalStateException e1) {
				e1.printStackTrace();
			} catch (SecurityException e1) {
				e1.printStackTrace();
			} catch (SystemException e1) {
				e1.printStackTrace();
			}
			return false;
		} finally {
			shutdown();
		}
	}
}