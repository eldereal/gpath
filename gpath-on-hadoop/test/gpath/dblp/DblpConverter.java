package gpath.dblp;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class DblpConverter {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: java Parser [input] [output]");
			System.exit(0);
		}
		PrintWriter writer = null;
		try{
			writer = new PrintWriter(new BufferedWriter(new FileWriter(args[1])));
			Parser p = new Parser(args[0], writer);
		}finally{
			if(writer!=null)writer.close();
		}
		
	}

}
