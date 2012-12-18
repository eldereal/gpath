package gpath.impl.hadoop;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;


public class GPathLogSequenceFileInputFormat extends SequenceFileInputFormat<NullWritable, LogWritable> {

}
