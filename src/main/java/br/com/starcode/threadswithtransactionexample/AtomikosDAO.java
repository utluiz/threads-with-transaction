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
			//prepara conexão
			XAConnection xac = null;
			Connection c = null;
			synchronized (transaction) {
				
				//AtomikosDataSource.getTM().begin();
				xac = AtomikosDataSource.getDS().getXaDataSource().getXAConnection();
				transaction.enlistResource(xac.getXAResource());
				c = xac.getConnection(); 
				
			}

			//executa normalmente
			Statement s = c.createStatement();
			s.executeUpdate("update teste set processado = '" + (falhar ? "falha" : "ok") + "' where id = " + id);
			s.close();
			c.close();
			
			//transaction.delistResource(xac.getXAResource(), XAResource.TMSUCCESS);
			return id;
		}
		
	}

	public static int processar(boolean falhar) {
		int ok = -1;
		Transaction transaction = null;
		try {
			transaction = AtomikosDataSource.getTM().getTransaction();

			ExecutorService executor = Executors.newFixedThreadPool(5);
			List<Callable<Integer>> processos = new ArrayList<Callable<Integer>>();
			for (int i = 0; i < 5; i++) {
				System.out.println(i);
				processos.add(new Processamento(i + 1, i == 4 && falhar, transaction));
			}
			System.out.println("### Iniciando ");
			List<Future<Integer>> futures = executor.invokeAll(processos);
			System.out.println("### OK ");
			ok = countOk(); 
			for (Future<Integer> future : futures) {
				System.out.println("Thread " + future.get() + " sucesso!");
			}

			
			transaction.commit();
			System.out.println("Sucesso geral!");
		} catch (Throwable e) {
			e.printStackTrace();
			try {
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