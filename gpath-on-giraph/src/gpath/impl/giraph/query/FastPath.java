package gpath.impl.giraph.query;

import java.io.DataOutput;
import java.io.IOException;

public abstract class FastPath {

	public abstract boolean contains(int l);
	public abstract int length();
	public abstract int get(int i);
	public abstract void writePath(DataOutput out) throws java.io.IOException;
	public abstract int writeToArray(int[] array, int offset);
	static final NullPath empty=new NullPath();
	
	public static FastPath empty(){
		return empty;
	}
	
	public static FastPath cat(FastPath p, int l1, int l2){
		return new Appending2Path(l1, l2, p);
	}
	
	public static FastPath cat(FastPath p, int l1){
		return new Appending1Path(l1, p);
	}
	
	public static FastPath cat(int l1, FastPath p){
		return new Prepending1Path(l1, p);
	}
	
	public static FastPath cat(int l1, int l2, FastPath p){
		return new Prepending2Path(l1, l2, p);
	}
	
	public static FastPath cat(int l1, FastPath p, int l2){
		return new DoublePending(l1, l2, p);
	}
	
	public static FastPath forArray(int[] array){
		return new ArrayPath(array);
	}
}

class ArrayPath extends FastPath{
	
	int[] array;
	
	public ArrayPath(int[] array){
		this.array = array;
	}

	@Override
	public boolean contains(int l) {
		for(int i=0;i<array.length;i++){
			if(array[i]==l){
				return true;
			}
		}
		return false;
	}

	@Override
	public int length() {
		return array.length;
	}

	@Override
	public void writePath(DataOutput out) throws IOException {
		for(int i=0;i<array.length;i++){
			out.writeLong(array[i]);
		}
	}

	@Override
	public int get(int i) {
		return array[i];
	}

	@Override
	public int writeToArray(int[] arr, int offset) {
		for(int i=0;i<array.length;i++){
			arr[offset+i] = array[i];
		}
		return array.length;
	}
	
	
}

class NullPath extends FastPath{
	
	@Override
	public boolean contains(int l) {
		return false;
	}

	@Override
	public int length() {
		return 0;
	}

	@Override
	public void writePath(DataOutput out) throws IOException {
		
	}

	@Override
	public int get(int i) {
		throw new java.lang.ArrayIndexOutOfBoundsException();
	}

	@Override
	public int writeToArray(int[] array, int offset) {
		return 0;
	}
}

class Appending2Path extends FastPath{
	// subpath l1 l2
	int l1,l2;
	FastPath subpath;
	public Appending2Path(int l1, int l2, FastPath subpath) {
		super();
		this.l1 = l1;
		this.l2 = l2;
		this.subpath = subpath;
	}
	
	@Override
	public boolean contains(int l) {
		return subpath.contains(l)||l1==l||l2==l;
	}

	@Override
	public int length() {
		return subpath.length()+2;
	}

	@Override
	public void writePath(DataOutput out) throws IOException {
		subpath.writePath(out);
		out.writeLong(l1);
		out.writeLong(l2);
	}

	@Override
	public int get(int i) {
		int l = subpath.length();
		if(i<l){
			return subpath.get(i);
		}else{
			if(i==l){
				return l1;
			}else if(i==l+1){
				return l2;
			}else{
				throw new java.lang.ArrayIndexOutOfBoundsException();
			}
		}
	}

	@Override
	public int writeToArray(int[] array, int offset) {
		int l = subpath.writeToArray(array, offset);
		array[offset+l]=l1;
		array[offset+l+1]=l2;
		return l+2;
	}
}

class Appending1Path extends FastPath{
	// subpath l1
	int l1;
	FastPath subpath;
	public Appending1Path(int l1, FastPath subpath) {
		super();
		this.l1 = l1;
		this.subpath = subpath;
	}
	
	@Override
	public boolean contains(int l) {
		return subpath.contains(l)||l1==l;
	}

	@Override
	public int length() {
		return subpath.length()+1;
	}
	
	@Override
	public void writePath(DataOutput out) throws IOException {
		subpath.writePath(out);
		out.writeLong(l1);
	}
	
	@Override
	public int get(int i) {
		int l = subpath.length();
		if(i<l){
			return subpath.get(i);
		}else{
			if(i==l){
				return l1;
			}else{
				throw new java.lang.ArrayIndexOutOfBoundsException();
			}
		}
	}

	@Override
	public int writeToArray(int[] array, int offset) {
		int l = subpath.writeToArray(array, offset);
		array[offset+l]=l1;
		return l+1;
	}
	
	
}

class DoublePending extends FastPath{
	// l1 subpath  l2
	
	int l1,l2;
	FastPath subpath;
	
	public DoublePending(int l1, int l2, FastPath subpath) {
		super();
		this.l1 = l1;
		this.l2 = l2;
		this.subpath = subpath;
	}
	
	@Override
	public boolean contains(int l) {
		return subpath.contains(l)||l1==l||l2==l;
	}

	@Override
	public int length() {
		return subpath.length()+2;
	}
	
	@Override
	public void writePath(DataOutput out) throws IOException {
		out.writeLong(l1);
		subpath.writePath(out);
		out.writeLong(l2);
	}
	
	@Override
	public int get(int i) {
		int l = subpath.length();
		if(i>0&&i<=l){
			return subpath.get(i-1);
		}else{
			if(i==0){
				return l1;
			}else if(i==l+1){
				return l2;
			}else{
				throw new java.lang.ArrayIndexOutOfBoundsException();
			}
		}
	}

	@Override
	public int writeToArray(int[] array, int offset) {
		int l = subpath.writeToArray(array, offset+1);
		array[0]=l1;
		array[offset+1+l]=l2;
		return l+2;
	}
}

class Prepending2Path extends FastPath{
	// l1 l2 subpath
	int l1,l2;
	FastPath subpath;
	
	public Prepending2Path(int l1, int l2, FastPath subpath) {
		super();
		this.l1 = l1;
		this.l2 = l2;
		this.subpath = subpath;
	}
	
	@Override
	public boolean contains(int l) {
		return subpath.contains(l)||l1==l||l2==l;
	}

	@Override
	public int length() {
		return subpath.length()+2;
	}
	
	@Override
	public void writePath(DataOutput out) throws IOException {
		out.writeLong(l1);
		out.writeLong(l2);
		subpath.writePath(out);
	}
	
	@Override
	public int get(int i) {
		int l = subpath.length();
		if(i<l+2&&i>1){
			return subpath.get(i-2);
		}else{
			if(i==0){
				return l1;
			}else if(i==1){
				return l2;
			}else{
				throw new java.lang.ArrayIndexOutOfBoundsException();
			}
		}
	}

	@Override
	public int writeToArray(int[] array, int offset) {
		int l = subpath.writeToArray(array, offset+2);
		array[0]=l1;
		array[1]=l2;
		return l+2;
	}
}

class Prepending1Path extends FastPath{
	// l1 subpath 
	int l1;
	FastPath subpath;
	public Prepending1Path(int l1, FastPath subpath) {
		super();
		this.l1 = l1;
		this.subpath = subpath;
	}
	
	@Override
	public boolean contains(int l) {
		return subpath.contains(l)||l1==l;
	}

	@Override
	public int length() {
		return subpath.length()+1;
	}
	
	@Override
	public void writePath(DataOutput out) throws IOException {
		out.writeLong(l1);
		subpath.writePath(out);
	}
	
	@Override
	public int get(int i) {
		int l = subpath.length();
		if(i<l+1&&i>0){
			return subpath.get(i-1);
		}else{
			if(i==0){
				return l1;
			}else{
				throw new java.lang.ArrayIndexOutOfBoundsException();
			}
		}
	}
	
	@Override
	public int writeToArray(int[] array, int offset) {
		int l = subpath.writeToArray(array, offset+1);
		array[0]=l1;
		return l+1;
	}
}
