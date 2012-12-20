package gpath.query.convert;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenRewriteStream;

import gpath.exception.BadQueryException;
import gpath.query.expression.Expression;
import gpath.query.parser.QueryLexer;
import gpath.query.parser.QueryParser;
import gpath.query.stm.SimpleStateMachine;
import gpath.query.stm.StateMachine;

public class NaiveQueryProcessor implements QueryProcessor {

	@Override
	public StateMachine compile(String query) throws BadQueryException {
		try{
			QueryLexer lexer=new QueryLexer(new ANTLRStringStream(query));
			QueryParser parser = new QueryParser(new TokenRewriteStream(lexer));
			Expression q = QueryCompletion.complete(parser.expression());
			SimpleStateMachine stm = QueryToStateMachine.convert(q);
			StateMachine fstm = stm.buildStateMachine();
			return fstm;
		}catch (RecognitionException e) {
			throw new BadQueryException(query, e);
		}
	}

}
