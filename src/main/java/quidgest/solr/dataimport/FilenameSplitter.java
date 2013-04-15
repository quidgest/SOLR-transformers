package quidgest.solr.dataimport;
 
import java.util.Map;
import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.Transformer;
 
public class FilenameSplitter extends Transformer {
 
        public Object transformRow(Map<String, Object> row, Context arg1) {
                String filename = ((String) row.get("file"));
                Split(row,filename);
                return row;
        }
 
        public static void Split(Map<String, Object> row, String filename) {
                int count = 0;
                String list = new String();
                String extension = "";
                // your previous split
                String[] Split1 = filename
                                .split("(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])|(?<=[a-zA-Z])(?=[0-9])|(_)|(\\.)");
                // I will store split results in list (I don't know size of array)
                for (String s : Split1) {
                        count++;
                        if (count < Split1.length) {
                                if (count == 1) {
                                        list = list + s;
                                }
                                else {
                                        list = list + " " + s;
                                }
                        }
                        else {
                                extension = s;
                        }
                }
                row.put("filenameSplited",list);
                row.put("extension", extension);
        }      
}