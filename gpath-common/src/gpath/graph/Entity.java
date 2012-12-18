package gpath.graph;

public interface Entity {
	
	public int getId();
	public String getType();
	public Iterable<LabelValue> getLabels();
	public byte[] getLabel(String name);
	public int getLabelCount();
}
