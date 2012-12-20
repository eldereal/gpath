package gpath.impl.giraph;

import java.util.concurrent.ExecutionException;

import gpath.exception.BadQueryException;
import gpath.impl.giraph.query.GPathQueryBSP;
import gpath.query.convert.NaiveQueryProcessor;
import gpath.query.stm.StateMachine;

public class QueryTest {
	public static void main(String[] args) throws BadQueryException, InterruptedException, ExecutionException {
		StateMachine stm = new NaiveQueryProcessor().compile(".");
		GPathBSP.runBsp("Query: \".\"", Conf.getDefaultConfiguration("local-mapreduce"), 
				new GPathQueryBSP(stm)).get();
	}
}
