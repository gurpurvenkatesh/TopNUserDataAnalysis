import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends Reducer<NullWritable, Text, Text, LongWritable>{

	private Integer FilterCount;
	private TreeMap<Integer, Text> TopMap = new TreeMap<Integer, Text>();
	
	protected void setup(Context context) throws IOException, InterruptedException {
		
		String strFilterCount = context.getConfiguration().get("filter_n_records");
		FilterCount = Integer.parseInt(strFilterCount);	
	}
	
	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		for(Text val : values) {		
			Map<String, String> parsed = MRUtils.transformXmlToMap(val.toString());
			
			String reputation = parsed.get("Reputation");			/* Extract Reputation */
			if(reputation == null)
				return;
			
			TopMap.put(Integer.parseInt(reputation), new Text(parsed.get("DisplayName")));		/* Extract DisplayName */
			if(TopMap.size() > FilterCount){
				TopMap.remove(TopMap.firstKey());					/* Reject the minimal Reputation key */
			}	
		} 

		for(Integer keys : TopMap.descendingMap().keySet()){			/* Looping the map in Descending order */
			context.write(TopMap.get(keys), new LongWritable(keys));	/* Writing key & value */
		} 
		
	}
}
