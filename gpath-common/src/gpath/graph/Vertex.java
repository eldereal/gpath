package gpath.graph;

public interface Vertex extends Entity{
	public Iterable<Edge> getOutEdges();
	public Iterable<Edge> getInEdges();
	public Iterable<Edge> getEdges();
	public int getOutEdgeCount();
	public int getInEdgeCount();
	public int getEdgeCount();
}
