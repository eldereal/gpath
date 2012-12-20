package gpath.impl.giraph.query;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gpath.graph.Edge;
import gpath.graph.Vertex;
import gpath.impl.giraph.GPathBSP;
import gpath.impl.giraph.GPathMessage;
import gpath.impl.giraph.GPathVertex;
import gpath.impl.hadoop.IntWritableComparable;
import gpath.query.stm.StateMachine;

public class GPathQueryBSP extends GPathBSP {

	StateMachine stm;
	static final Log LOG = LogFactory.getLog(GPathQueryBSP.class);
	
	public GPathQueryBSP(){
		this.stm = new StateMachine();
	}
	
	public GPathQueryBSP(StateMachine stm){
		this.stm = stm;
	}
	
	@Override
	public void compute(long superstep, GPathVertex vertex,
			Iterable<GPathMessage> msgIterator) throws IOException {
		boolean halt = true;
		if(superstep==0){
			doQueryForVertex(superstep, vertex, new GPathQueryMessage(stm.getStartState()));
			halt = false;
		}else{
			for(GPathMessage msg:msgIterator){
				doQueryForVertex(superstep, vertex, new GPathQueryMessage(msg));
				halt = false;
			}
		}
		if(halt)vertex.voteToHalt();
	}	
	
	private void doQueryForVertex(long superstep, GPathVertex vertex,
			GPathQueryMessage msg) {		
		int stateId = msg.getStateId();
		int backtrack = msg.getBacktrackCondition();
		FastPath path = msg.getPath();
		StateMachine.State state = stm.getState(stateId);
		int condIndex = 0;
		Vertex v = new GPathDelegateVertex(vertex);
		for (StateMachine.Condition cond : state.getConditions()) {
			if(stateId == stm.getStartState()){
				backtrack = condIndex;
			}
			condIndex++;
			if (cond.getTest().test(v)) {
				for (StateMachine.Transition t : cond.getTransitions()) {
					switch (t.getType()) {
					case In:
						e: for (Edge e : v.getInEdges()) {
							if (t.getTest().test(e)) {
								int ovid = e.getInVertex().getId();
								if(path.contains(ovid)){
									continue e;
								}
								int newState = t.getToState();
								sendMessageToVertex(vertex, ovid, newState, backtrack, FastPath.cat(e.getId(), v.getId(), path));
							}
						}
						break;
					case Out:
						e: for (Edge e : v.getOutEdges()) {
							if (t.getTest().test(e)) {
								int ovid = e.getOutVertex().getId();
								if(path.contains(ovid)){
									continue e;
								}
								int newState = t.getToState();
								sendMessageToVertex(vertex, ovid, newState, backtrack, FastPath.cat(path, v.getId(), e.getId()));
							}
						}
						break;
					case Backtrack:{
//						int bstate = t.getToState();
//						int btrack = condIndex;
//						StateMachine.Condition bcond = stm.getState(bstate).getCondition(backtrack);
//						for (StateMachine.Transition bt : bcond.getTransitions()) {
//							switch (bt.getType()) {
//							case In:{
//								Vertex bv = graphStore.getVertex(path.get(0));
//								e: for (Edge e : bv.getInEdges()) {
//									if (bt.getTest().test(e)) {
//										int ovid = e.getInVertex().getId();
//										if(path.contains(ovid)){
//											continue e;
//										}
//										int newState = bt.getToState();
//										sendMessageToVertex(ovid, newState, btrack, FastPath.cat(e.getId(), path, vid));
//									}
//								}
//							}break;
//							case Out:{
//								Vertex bv = graphStore.getVertex(path.get(path.length()-1));
//								e: for (Edge e : bv.getOutEdges()) {
//									if (bt.getTest().test(e)) {
//										int ovid = e.getOutVertex().getId();
//										if(path.contains(ovid)){
//											continue e;
//										}
//										int newState = bt.getToState();
//										sendMessageToVertex(ovid, newState, btrack, FastPath.cat(vid, path, e.getId()));
//									}
//								}
//							}break;
//							case InSuccess:{
//								container.sendResult(sessionId, FastPath.cat(path, vid));
//							}break;
//							case OutSuccess:{
//								container.sendResult(sessionId, FastPath.cat(vid, path));
//							}break;
//							default:
//								throw new HdglException("bad state");
//							}
//						}
						throw new UnsupportedOperationException();
					}	
					case OutSuccess:{
						sendResult(FastPath.cat(path, v.getId()));
					}break;
					case InSuccess:{
						sendResult(FastPath.cat(v.getId(), path));
					}break;
					default:

					}
				}
				break;
			}
		}
		
	}

	private void sendResult(FastPath cat) {
		int[] path=new int[cat.length()];
		cat.writeToArray(path, 0);
		LOG.info("result:" + Arrays.toString(path));
	}

	private void sendMessageToVertex(GPathVertex vertex, int vid, int newState, int backtrack,
			FastPath path) {
		GPathMessage msg = new GPathMessage();
		msg.setContent(new GPathQueryMessage(newState,backtrack,path));
		vertex.sendMessage(new IntWritableComparable(vid), msg);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		stm.readFields(in);
	}
	
	public void write(java.io.DataOutput out) throws IOException {
		stm.write(out);
	};

}
