package gpath.graph;

public interface Edge extends Entity {

	Vertex getInVertex();
	Vertex getOutVertex();
	Vertex getOtherVertex(Vertex one);

}
