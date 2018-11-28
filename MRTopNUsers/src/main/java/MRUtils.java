import java.util.HashMap;
import java.util.Map;

public class MRUtils {

public static final String[] REDIS_INSTANCES = {"p0","p1","p2","p3","p4","p6"};
	
	// This helper functional parses StackOverflow into Map for us
	
	public static Map<String, String> transformXmlToMap(String xml){
		
		Map<String, String> map = new HashMap<String, String>(); 	/* Output is returned in the form of Key, Value map objects*/
		try {
			
			String[] tokens = xml.trim().substring(5, xml.trim().length() -3).split("\"");		
			/* XLML parses the attributes tokenized through trims the xml string from first 5 strings till 3rd letter before the ending */
			
			for(int i=0; i<tokens.length-1; i+=2) {
				String key = tokens[i].trim();
				String val = tokens[i+1];
				
				map.put(key.substring(0, key.length()-1), val);
				
				/* Sample output
				 * Uerdid : 101
				 * Comment : I am God
				 * Comment Date : 02/08/2018 */
				
			}
		}catch(StringIndexOutOfBoundsException e) {
			
			System.err.println(xml);
		}
		
		return map;
	}
}
