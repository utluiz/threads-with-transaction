package br.com.starcode.threadswithtransactionexample;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class AtomikosTest {
	
	@BeforeClass
	public static void init() {
		//create atomikos transaction manager and data source
		AtomikosDataSource.init();
		
	}
	@Before
	public void reset() {
		//recreate data of TEST table
		AtomikosDAO.resetTable();
	}
	
	@AfterClass
	public static void shutdown() {
		//close atomikos resources
		AtomikosDataSource.shutdown();
	}
	
	@Test
	public void sucesso() {
		//process 5 rows in 5 threads
		int okParcial = AtomikosDAO.processar(false);
		//should return 5 successes
		Assert.assertEquals(5, okParcial);
		//confirms in table, count 5 ok's
		Assert.assertEquals(5, AtomikosDAO.countOk());
	}
	
	@Test
	public void fail() {
		//process 5 rows in 5 threads, one should fail
		int okParcial = AtomikosDAO.processar(true);
		//should return 4 successes
		Assert.assertEquals(4, okParcial);
		//confirms in table, count zero ok's due to rollback
		Assert.assertEquals(0, AtomikosDAO.countOk());
	}
	
}
