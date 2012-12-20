package gpath.store;

import java.util.Comparator;

import gpath.store.statistics.LabelStatistics;
import gpath.store.statistics.Statistics;

public interface StatisticsGraphStore extends GraphStore{

	public Statistics getVertexStatistics(String labelname);	

	public Statistics getEdgeStatistics(String labelname);	
	
	public <T extends Comparator<byte[]>> void prepareVertexStatistics(Class<T> comparatorClass, String labelName);
	
	public <T extends Comparator<byte[]>> void prepareEdgeStatistics(Class<T> comparatorClass, String labelName);
	
}
