package gpath.graph;

import java.io.IOException;

public interface Graph {

	public Iterable<Path> query(String queryRegex);
	
	public MutableGraph beginModify() throws IOException;
	
}
