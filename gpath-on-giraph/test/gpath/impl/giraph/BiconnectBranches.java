package gpath.impl.giraph;

import gpath.impl.hadoop.IntWritableComparable;
import gpath.util.WritableHelper;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.giraph.graph.Edge;

public class BiconnectBranches {

	public static class BiconnectBranchesBSP extends GPathBSP{

		@Override
		public void compute(long superstep, GPathVertex vertex,
				Iterable<GPathMessage> msgIterator) throws IOException {
			IntWritableComparable minimal = new IntWritableComparable(vertex.getId().get());
			boolean modified = superstep == 0;
			if(vertex.getValue().containsKey("minimalBranch")){
				minimal.set(WritableHelper.parseInt(vertex.getValue().get("minimalBranch")));
			}
			for(GPathMessage msg:msgIterator){
				int min = WritableHelper.parseInt(msg.getContent());
				if(min < minimal.get()){
					minimal.set(min);
					modified = true;
				}
			}			
			if(!modified){
				vertex.voteToHalt();
			}else{
				GPathMessage msg = new GPathMessage();
				msg.setContent(WritableHelper.toBytes(minimal.get()));
				for(Edge<IntWritableComparable, GPathEdge> edge:vertex.getEdges()){
					GPathEdge e = edge.getValue();
					if(e.hasInEdge&&e.hasOutEdge){
						vertex.sendMessage(edge.getTargetVertexId(), msg);
					}
				}
				vertex.getValue().put("minimalBranch", WritableHelper.toBytes(minimal.get()));
			}
		}
		
	}
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		GPathBSP.runBsp("BiconnectBranches", Conf.getDefaultConfiguration("local-mapreduce"), BiconnectBranchesBSP.class).get();
	}

}
