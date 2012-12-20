package gpath.query.convert;

import gpath.exception.BadQueryException;
import gpath.query.stm.StateMachine;

public interface QueryProcessor {

	public StateMachine compile(String query) throws BadQueryException;
	
}
