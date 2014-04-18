import org.junit.Assert;
import org.junit.Test;


public class AtomikosExampleTest {
	
	@Test
	public void sucesso() {
		Assert.assertTrue(new AtomikosExample().processar(false));
	}
	
	@Test
	public void fail() {
		Assert.assertFalse(new AtomikosExample().processar(true));
	}

}
