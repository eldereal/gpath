package gpath.impl.giraph.query;

import gpath.impl.giraph.GPathMessage;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class GPathQueryMessage implements Writable {

	int stateId;
	int backtrack;
	FastPath path;

	public GPathQueryMessage(){
		path = FastPath.empty();
	}
	
	public GPathQueryMessage(int startState) {
		stateId = startState;
		path = FastPath.empty();
	}

	public GPathQueryMessage(GPathMessage msg) {
		try {
			this.readFields(new DataInputStream(new ByteArrayInputStream(msg.getContent())));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	
	
	public GPathQueryMessage(int stateId, int backtrack, FastPath path) {
		super();
		this.stateId = stateId;
		this.backtrack = backtrack;
		this.path = path;
	}

	public int getStateId() {
		return stateId;
	}

	public void setStateId(int stateId) {
		this.stateId = stateId;
	}

	public int getBacktrackCondition() {
		return backtrack;
	}

	public void setBacktrackCondition(int backtrack) {
		this.backtrack = backtrack;
	}

	public FastPath getPath() {
		return path;
	}

	public void setPath(FastPath path) {
		this.path = path;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		stateId = in.readInt();
		backtrack = in.readInt();
		int len = in.readInt();
		int[] path = new int[len];
		for(int i = 0;i<len;i++){
			path[i] = in.readInt();
		}
		this.path = FastPath.forArray(path);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(stateId);
		out.writeInt(backtrack);
		int len=path.length();
		out.writeInt(len);
		for(int i=0;i<len;i++){
			out.writeInt(path.get(i));
		}
	}
	
}
