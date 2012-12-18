package gpath.graph;

import java.io.IOException;

import gpath.task.AsyncResult;

public interface MutableGraph {

	public AsyncResult<Boolean> commit();
	public AsyncResult<Boolean> abort();
	
	public int createVertex(String type)throws IOException;
	public int createEdge(String type, int start, int end)throws IOException;
	public int createEdge(String type, Vertex start, Vertex end)throws IOException;
	
	public void deleteEntity(Entity e);
	public void deleteLabel(Entity e, String name);
	
	void setVertexLabel(int entity, String name, byte[] value)throws IOException;
	void setVertexLabel(Vertex vertex, String name, byte[] value)throws IOException;
	void setEdgeLabel(int entity, int start, int end, String name, byte[] value)throws IOException;
	void setEdgeLabel(Edge edge, String name, byte[] value)throws IOException;
}
