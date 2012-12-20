package gpath.impl.hadoop;
import static org.junit.Assert.*;

import gpath.util.WritableHelper;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;


public class SmallGraphGen {

	@Test
	public void test() throws Exception {
		Configuration conf = Conf.getDefaultConfiguration("1.1.1");
		Graph g = new Graph(conf);
		gpath.graph.MutableGraph m = g.beginModify();
		Map<Integer, Integer> vids = new HashMap<Integer, Integer>();
		Map<Integer, Integer> eids = new HashMap<Integer, Integer>();
		vids.put(1, m.createVertex("person"));
		vids.put(2, m.createVertex("person"));
		vids.put(3, m.createVertex("person"));
		vids.put(4, m.createVertex("person"));
		vids.put(5, m.createVertex("person"));
		eids.put(1, m.createEdge("forward", vids.get(1), vids.get(2)));
		eids.put(2, m.createEdge("forward", vids.get(2), vids.get(3)));
		eids.put(3, m.createEdge("forward", vids.get(3), vids.get(4)));
		eids.put(4, m.createEdge("forward", vids.get(4), vids.get(5)));
		eids.put(5, m.createEdge("back", vids.get(5), vids.get(1)));
		eids.put(6, m.createEdge("jump", vids.get(1), vids.get(3)));
		eids.put(7, m.createEdge("jump", vids.get(1), vids.get(4)));
		eids.put(8, m.createEdge("jump", vids.get(2), vids.get(5)));
		eids.put(9, m.createEdge("back", vids.get(3), vids.get(2)));
		m.setVertexLabel(vids.get(1), "name", WritableHelper.toBytes("Wang"));
		m.setVertexLabel(vids.get(2), "name", WritableHelper.toBytes("Zhang"));
		m.setVertexLabel(vids.get(3), "name", WritableHelper.toBytes("Lin"));
		m.setVertexLabel(vids.get(4), "name", WritableHelper.toBytes("Qian"));
		m.setVertexLabel(vids.get(5), "name", WritableHelper.toBytes("Wen"));
		m.setEdgeLabel(eids.get(1), vids.get(1), vids.get(2), "len", WritableHelper.toBytes(1));
		m.setEdgeLabel(eids.get(2), vids.get(2), vids.get(3), "len", WritableHelper.toBytes(1));
		m.setEdgeLabel(eids.get(3), vids.get(3), vids.get(4), "len", WritableHelper.toBytes(1));
		m.setEdgeLabel(eids.get(4), vids.get(4), vids.get(5), "len", WritableHelper.toBytes(1));
		m.setEdgeLabel(eids.get(5), vids.get(5), vids.get(1), "len", WritableHelper.toBytes(-4));
		m.setEdgeLabel(eids.get(6), vids.get(1), vids.get(3), "len", WritableHelper.toBytes(2));
		m.setEdgeLabel(eids.get(7), vids.get(1), vids.get(4), "len", WritableHelper.toBytes(3));
		m.setEdgeLabel(eids.get(8), vids.get(2), vids.get(5), "len", WritableHelper.toBytes(3));
		m.setEdgeLabel(eids.get(9), vids.get(3), vids.get(2), "len", WritableHelper.toBytes(-1));
		assertTrue(m.commit().get());
		
	}

	public static void main(String[] args) throws Exception {
		new SmallGraphGen().test();
	}
	
}
