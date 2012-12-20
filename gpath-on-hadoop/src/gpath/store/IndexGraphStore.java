package gpath.store;

import java.util.Comparator;

public interface IndexGraphStore extends StatisticsGraphStore {

	public Iterable<Long> findVertexByLabelInRange(String label, byte[] min, byte[] max);
	
	public Iterable<Long> findEdgeByLabelInRange(String label, byte[] min, byte[] max);
	
	public Iterable<Long> findVertexByLabelValue(String label, byte[] val);
	
	public Iterable<Long> findEdgeByLabelValue(String label, byte[] val);
	
	public void prepareVertexIndex(String name, Comparator<byte[]> comparator);
	
	public void prepareEdgeIndex(String name, Comparator<byte[]> comparator);
}
