package wikipedia.org;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CalculateTotalLinksMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		context.getInputSplit();
		String merge;
		String line = lineText.toString();
		String title_array[]={" "};
		try {
			title_array=titleCount(lineText);
			if(title_array[0].equals(" "))
				context.write(new Text("Count of Pages"),new IntWritable(0));
				else
					context.write(new Text("Count of Pages"),new IntWritable(1));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private String[] titleCount(Text line) throws Exception {
		// TODO Auto-generated method stub
		String retrieve_title[]=new String[2];String lineString=line.toString();
		if(lineString.contains("<title>")&&lineString.contains("</title>")){
		int begin_index=line.find("<title>");
		
		int end_index=line.find("</title>",begin_index);
		begin_index=begin_index+7;//System.out.println(begin_index+"begin");System.out.println(end_index+"end");
		int temp=end_index-begin_index;
		if(begin_index!=end_index&&end_index>begin_index){
		retrieve_title[0]=Text.decode(line.getBytes(), begin_index, temp);}
		else
			retrieve_title[0]=" ";retrieve_title[1]=" ";
		}
		else
			{retrieve_title[0]=" ";retrieve_title[1]=" ";}
		return retrieve_title;
	}
}