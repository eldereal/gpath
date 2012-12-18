package gpath.impl.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;

public class LogWriter {

	SequenceFile.Writer writer;
	
	public LogWriter(FileSystem fs, Configuration conf, Path name) throws IOException {
		writer = SequenceFile.createWriter(fs, conf, name, NullWritable.class, LogWritable.class);
	}
	
	public void writeLog(LogWritable log) throws IOException{
		writer.append(NullWritable.get(), log);
	}
	
	public void close() throws IOException{
		writer.close();
	}
	
}
