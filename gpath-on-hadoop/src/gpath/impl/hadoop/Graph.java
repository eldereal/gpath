package gpath.impl.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import gpath.graph.MutableGraph;
import gpath.graph.Path;

public class Graph implements gpath.graph.Graph {

	
	Configuration conf;
	
	public Graph(Configuration conf){
		this.conf = conf;
	}
	
	@Override
	public Iterable<Path> query(String queryRegex) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MutableGraph beginModify() throws IOException {
		return new gpath.impl.hadoop.MutableGraph(conf);
	}

}
