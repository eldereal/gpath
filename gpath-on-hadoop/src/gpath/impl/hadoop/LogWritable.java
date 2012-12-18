package gpath.impl.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class LogWritable implements Writable {
	
	static final byte ADD_VERTEX = 0;
	static final byte ADD_EDGE = 1;
	static final byte SET_VERTEX_LABEL = 2;
	static final byte SET_EDGE_LABEL = 3;
	//static final byte DELETE_ENTITY = 4;
	//static final byte DELETE_LABEL = 5;
	
	public static LogWritable addVertex(int tempId, String oftype){
		return new LogWritable(ADD_VERTEX, tempId, 0, 0, oftype, null);
	}
	
	public static LogWritable addEdge(int tempId, String oftype, int v1, int v2){
		return new LogWritable(ADD_EDGE, tempId, v1, v2, oftype, null);
	}
	
	public static LogWritable setVertexLabel(int entity, String name, byte[] value){
		return new LogWritable(SET_VERTEX_LABEL, entity, 0, 0, name, value);
	}
	
	public static LogWritable setEdgeLabel(int entity, int v1, int v2, String name, byte[] value){
		return new LogWritable(SET_EDGE_LABEL, entity, v1, v2, name, value);
	}
	
	byte type;
	int id1,id2,id3;
	String name;
	byte[] data;
	
	public byte getType() {
		return type;
	}

	public void setType(byte type) {
		this.type = type;
	}

	public int getId1() {
		return id1;
	}

	public void setId1(int id1) {
		this.id1 = id1;
	}

	public int getId2() {
		return id2;
	}

	public void setId2(int id2) {
		this.id2 = id2;
	}

	public int getId3() {
		return id3;
	}

	public void setId3(int id3) {
		this.id3 = id3;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public LogWritable(){
		
	}
	
	public LogWritable(byte type, int id1, int id2,int id3, String name, byte[] data) {
		super();
		this.type = type;
		this.id1 = id1;
		this.id2 = id2;
		this.id3 = id3;
		this.name = name;
		this.data = data;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		type = in.readByte();
		switch (type) {
		case ADD_VERTEX:
			id1 = in.readInt();
			name = in.readUTF();
			break;
		case ADD_EDGE:
			id1 = in.readInt();
			id2 = in.readInt();
			id3 = in.readInt();
			name = in.readUTF();
			break;
		case SET_VERTEX_LABEL:
			id1 = in.readInt();
			name = in.readUTF();
			int len=in.readInt();
			if(len>0){
				data = new byte[len];
				in.readFully(data);
			}else{
				data=null;
			}
			break;
		case SET_EDGE_LABEL:
			id1 = in.readInt();
			id2 = in.readInt();
			id3 = in.readInt();
			name = in.readUTF();
			len = in.readInt();
			if(len>0){
				data = new byte[len];
				in.readFully(data);
			}else{
				data=null;
			}
			break;
		default:
			throw new IllegalArgumentException("Illegal log type: "+ type);
		}
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(type);
		switch (type) {
		case ADD_VERTEX:
			out.writeInt(id1);
			out.writeUTF(name);
			break;
		case ADD_EDGE:
			out.writeInt(id1);
			out.writeInt(id2);
			out.writeInt(id3);
			out.writeUTF(name);	
			break;
		case SET_VERTEX_LABEL:
			out.writeInt(id1);
			out.writeUTF(name);
			if(data!=null){
				out.writeInt(data.length);
				out.write(data);
			}else{
				out.writeInt(0);
			}
			break;
		case SET_EDGE_LABEL:
			out.writeInt(id1);
			out.writeInt(id2);
			out.writeInt(id3);
			out.writeUTF(name);
			if(data!=null&&data.length>0){
				out.writeInt(data.length);
				out.write(data);
			}else{
				out.writeInt(0);
			}
			break;
		default:
			throw new IllegalArgumentException("Illegal log type: "+ type);
		}
		
	}
	
	
}
