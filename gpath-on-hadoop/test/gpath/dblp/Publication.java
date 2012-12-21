package gpath.dblp;

/*
 * Created on 07.06.2005
 */

import java.io.PrintWriter;
import java.util.*;

/**
 * @author ley
 *
 * created first in project xml5_coauthor_graph
 */
public class Publication {
    private static Set ps= new HashSet(650000);
    private static int maxNumberOfAuthors = 0;
    private static int allocId = 1;
    private String key;
    private Person[] authors;	// or editors
    private int id;

    public Publication(String key, Person[] persons, PrintWriter writer) {
        this.key = key;
        authors = persons;
        id=allocId++;
        ps.add(this);
        if (persons.length > maxNumberOfAuthors)
            maxNumberOfAuthors = persons.length;
        writer.print("Publication,"+id+","+key);
        for(Person p:persons){
        	writer.print(",");
        	writer.print(p.getId());
        }
        writer.println();
    }
    
    public static int getNumberOfPublications() {
        return ps.size();
    }
    
    public static int getMaxNumberOfAuthors() {
        return maxNumberOfAuthors;
    }
    
    public Person[] getAuthors() {
        return authors;
    }
    
    static Iterator iterator() {
        return ps.iterator();
    }
}