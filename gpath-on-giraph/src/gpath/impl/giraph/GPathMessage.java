package gpath.impl.giraph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class GPathMessage implements Writable {

	private byte[] content;
	
	public GPathMessage(){
		
	}	
	
	public byte[] getContent() {
		return content;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int len = in.readInt();
		content = new byte[len];
		in.readFully(content);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(content.length);
		out.write(content);
	}

}
