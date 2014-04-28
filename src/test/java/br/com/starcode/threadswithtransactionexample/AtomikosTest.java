package br.com.starcode.threadswithtransactionexample;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;


public class AtomikosTest {
	
	@BeforeClass
	public static void init() {
		AtomikosDataSource.init();
		
	}
	@Before
	public void reset() {
		AtomikosDAO.resetTable();
	}
	
	@AfterClass
	public static void shutdown() {
		AtomikosDataSource.shutdown();
	}
	
	@Test
	public void sucesso() {
		int okParcial = AtomikosDAO.processar(false);
		Assert.assertEquals(5, okParcial);
		Assert.assertEquals(5, AtomikosDAO.countOk());
	}
	
	@Test
	@Ignore
	public void fail() {
		int okParcial = AtomikosDAO.processar(true);
		Assert.assertEquals(4, okParcial);
		Assert.assertEquals(0, AtomikosDAO.countOk());
	}
	
}
