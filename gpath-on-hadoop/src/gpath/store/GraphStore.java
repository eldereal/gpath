package gpath.store;

import java.io.IOException;
import java.io.InputStream;

public interface GraphStore {

	public InputStream getVertexData(long id) throws IOException;
	
	public InputStream getEdgeData(long id) throws IOException;
	
	public gpath.graph.Vertex getVertex(long id) throws IOException;	
	
	public gpath.graph.Edge getEdge(long id) throws IOException;
	
	public gpath.graph.Vertex parseVertex(long id) throws IOException;	
	
	public gpath.graph.Edge parseEdge(long id) throws IOException;
	
	public String[] bestPlacesForVertex(long entityId) throws IOException ;
	
	public String[] bestPlacesForEdge(long entityId) throws IOException ;
	
	public long getVertexCount();
	
	public long getVertexCountPerBlock();
	
	public long getEdgeCount();
	
	public long getEdgeCountPerBlock();
	
	public void close();
}
