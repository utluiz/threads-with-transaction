package br.com.starcode.threadswithtransactionexample;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.sql.XAConnection;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

public class AtomikosDAO {

	/**
	 * Reset table content with 5 rows to be used each one for a different thread
	 */
	public static void resetTable() {
		try {
			Connection c = AtomikosDataSource.getDS().getConnection();
			Statement s = c.createStatement();
			s.executeUpdate("delete from teste");
			s.executeUpdate("insert into teste (id, nome, processado) values (1, 'Item 1', null)");
			s.executeUpdate("insert into teste (id, nome, processado) values (2, 'Item 2', null)");
			s.executeUpdate("insert into teste (id, nome, processado) values (3, 'Item 3', null)");
			s.executeUpdate("insert into teste (id, nome, processado) values (4, 'Item 4', null)");
			s.executeUpdate("insert into teste (id, nome, processado) values (5, 'Item 5', null)");
			s.close();
			c.close();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Count haw many rows are with ok flag
	 * @return
	 */
	public static int countOk() {
		try {
			Connection c = AtomikosDataSource.getDS().getConnection();
			Statement s = c.createStatement();
			ResultSet rs =  s.executeQuery("select count(1) from teste where processado = 'ok'");
			if (rs.next()) {
				return rs.getInt(1);
			}
			rs.close();
			s.close();
			c.close();
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return 0;
	}

	/**
	 * Processing thread.
	 * Tries to update the row with "ok"
	 */
	private static class Processamento implements Callable<Integer> {

		private int id;
		private boolean falhar;
		private Transaction transaction;
		
		public Processamento(int id, boolean falhar, Transaction transaction) {
			this.falhar = falhar;
			this.transaction = transaction;
			this.id = id;
		}
		
		public Integer call() throws Exception {
			if (falhar) {
				throw new RuntimeException("Falhou inesperadamente!");
			}
			
			//enlist xa connection
			XAConnection xac = AtomikosDataSource.getDS().getXaDataSource().getXAConnection();
			synchronized (transaction) {
				transaction.enlistResource(xac.getXAResource());
			}
			
			//normal execution, update row with OK
			Connection c = xac.getConnection();
			Statement s = c.createStatement();
			s.executeUpdate("update teste set processado = 'ok' where id = " + id);
			s.close();
			c.close();
			
			//delist xa connection
			synchronized (transaction) {
				transaction.delistResource(xac.getXAResource(), XAResource.TMSUCCESS);
			}
			return id;
		}
		
	}

	/**
	 * Starts 5 threads. Each thread update a row of the TEST table with OK. 
	 * @param falhar If true, the last thread will throw an exception.
	 * @return Total threads that result in success
	 */
	public static int processar(boolean falhar) {
		int ok = 0;
		Transaction transaction = null;
		try {
			
			//start transaction
			AtomikosDataSource.getTM().begin();
			transaction = AtomikosDataSource.getTM().getTransaction();

			//create thread pool
			ExecutorService executor = Executors.newFixedThreadPool(5);
			List<Callable<Integer>> processos = new ArrayList<Callable<Integer>>();
			
			//create 5 threads, passing the main transaction as argument
			for (int i = 0; i < 5; i++) {
				processos.add(new Processamento(i + 1, i == 4 && falhar, transaction));
			}
			
			//execute threads and wait
			List<Future<Integer>> futures = executor.invokeAll(processos);
			
			//count the result; get() will fail if thread threw an exception
			Throwable ex = null;
			for (Future<Integer> future : futures) {
				try {
					int threadId = future.get();
					System.out.println("Thread " + threadId + " sucesso!");
					ok++; 
				} catch (Throwable e) {
					ex = e;
				}
			}
			
			if (ex != null) {
				throw ex;
			}

			//finish transaction normally
			transaction.commit();
			
		} catch (Throwable e) {
			
			e.printStackTrace();
			try {
				//try to rollback
				if (transaction != null) {
					AtomikosDataSource.getTM().rollback();
				}
			} catch (IllegalStateException e1) {
				e1.printStackTrace();
			} catch (SecurityException e1) {
				e1.printStackTrace();
			} catch (SystemException e1) {
				e1.printStackTrace();
			}
			
		}
		return ok;
	}
	
}