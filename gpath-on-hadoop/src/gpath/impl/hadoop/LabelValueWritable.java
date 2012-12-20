package gpath.impl.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class LabelValueWritable implements Writable, Map<String, byte[]> {
	
	HashMap<String, byte[]> map = new HashMap<String, byte[]>();

	public LabelValueWritable(){
		
	}
	
	public LabelValueWritable(LabelValueWritable labelValues) {
		for(Map.Entry<String, byte[]> e:labelValues.entrySet()){
			map.put(e.getKey(), e.getValue());
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int len = in.readInt();
		map.clear();
		for(int i=0;i<len;i++){
			String s=in.readUTF();
			int l=in.readInt();
			byte[] b=new byte[l];
			in.readFully(b);
			map.put(s, b);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(map.size());
		for(Map.Entry<String, byte[]> v:map.entrySet()){
			out.writeUTF(v.getKey());
			byte[] a = v.getValue();
			out.writeInt(a.length);
			out.write(a);
		}		
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	@Override
	public Set<java.util.Map.Entry<String, byte[]>> entrySet() {
		return map.entrySet();
	}

	@Override
	public byte[] get(Object key) {
		return map.get(key);
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public Set<String> keySet() {
		return map.keySet();
	}

	@Override
	public byte[] put(String key, byte[] value) {
		return map.put(key, value);
	}

	@Override
	public void putAll(Map<? extends String, ? extends byte[]> m) {
		map.putAll(m);
	}

	@Override
	public byte[] remove(Object key) {
		return map.remove(key);
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public Collection<byte[]> values() {
		return map.values();
	}
	
	
}
