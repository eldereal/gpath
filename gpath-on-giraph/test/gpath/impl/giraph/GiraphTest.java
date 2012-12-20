package gpath.impl.giraph;

import java.io.IOException;
import java.util.Iterator;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class GiraphTest {

	public static class NullBSP extends GPathBSP{
		
		static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(NullBSP.class);
		
		@Override
		public void compute(long superstep, GPathVertex vertex,
				Iterable<GPathMessage> msgIterator) throws IOException {
			log.info("enter superstep "+superstep+", vertex "+vertex.getId().get());
			if(superstep >= 5){
				vertex.voteToHalt();
			}
		}
	}
	
	static Configuration conf;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		conf = Conf.getDefaultConfiguration("1.1.1");
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void test() throws Exception{
		assertTrue(GPathBSP.runBsp(getClass().getName(), conf, new NullBSP()).get());
	}
	
	public static void main(String[] args) throws Exception {
		setUpBeforeClass();
		new GiraphTest().test();
		tearDownAfterClass();
	}

}
