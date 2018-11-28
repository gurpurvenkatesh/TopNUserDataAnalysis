import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopMapper extends Mapper<Object, Text, NullWritable, Text>{

	private Integer FilterCount;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		
		String strFilterCount = context.getConfiguration().get("filter_n_records");		/* extracting configuration property */
		FilterCount = Integer.parseInt(strFilterCount);
		
	}
	
	private TreeMap<Integer, Text> TopMap = new TreeMap<Integer, Text>();
			
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		Map<String, String> parsed = MRUtils.transformXmlToMap(value.toString());
		
		String reputation = parsed.get("Reputation");
		if(reputation == null)
			return;
		
		TopMap.put(Integer.parseInt(reputation), new Text(value));	/* Reputation score as key & the input xml as value */
		
		if(TopMap.size() > FilterCount)
				TopMap.remove(TopMap.firstKey());		/* Removing topmost entry which will remove lowest reputation score */
		
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		
		for(Text t : TopMap.values()){					/* Traversing map & writing them into individual key value pairs */
			context.write(NullWritable.get(), t);		/* Note that the entire line of xml is sent as Mapper output */
		}
	}
}
