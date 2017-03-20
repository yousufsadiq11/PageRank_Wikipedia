package wikipedia.org;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalculateInitialPageRankMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		context.getInputSplit();
		boolean check = true;
		String title_array[] = { " " };
		Pattern matching_pattern = Pattern.compile("\\[.+?\\]");
		try {
			title_array = TextAndTitleRetrieval(lineText);
			if (title_array[0].equals(" ")) {// Do Nothing for empty String
			} 
			// Replace spaces with underscores in titles
			else {
				title_array[0]=title_array[0].replace(" ", "_");
				Matcher matcher = matching_pattern.matcher(title_array[1]);
				// Writing Valid Links to a file
				while (matcher.find()) {
					String links = matcher.group();
					links = outLinks(links);
					if (links == null || links.isEmpty())
						continue;
					check = false;
					context.write(new Text(title_array[0]), new Text(links));
				}
				// Writing empty in Links category if there are no outlinks
				if (!matcher.find() && check == true && !(title_array[0] == "")) {
					context.write(new Text(title_array[0]), new Text(""));			
				}
			}
		} 
		// Throwing Exceptions if any
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// Computing outlinks for the corresponding title
	private String outLinks(String links) {
		// TODO Auto-generated method stub
		// Checking for [[ which indicates start of outlinks
		boolean flag = links.startsWith("[[");
		int begin_text=0;
		if(flag==true)
			begin_text=begin_text+2;
		else
			begin_text=begin_text+1;
		// Retrieving data before the ] delimeter
		int end_text = links.indexOf("]");
		int part = links.indexOf("#");
		if (part > 0) {
			end_text = part;
		}
		// Checking if it contains |
		// Retrieving data before | if it is present in the string
		if (begin_text == 2) {
			links = links.substring(begin_text, end_text);
			if(links.contains("|")){
				int index=links.indexOf("|");
				links=links.substring(0,index);
			}
			links = links.replaceAll(" ", "_");
			return links;
		} 
		else {
			return "";
		}
	}

	// Retrieving Text and Titles from the XML
	private String[] TextAndTitleRetrieval(Text line) throws Exception {
		// TODO Auto-generated method stub
		String retrieve_title[] = new String[2];
		String lineString = line.toString();
		// Retrieving data between <title> and </title> tags
		if (lineString.contains("<title>") && lineString.contains("</title>")) {
			int begin_index = line.find("<title>");
			int end_index = line.find("</title>", begin_index);
			begin_index = begin_index + 7;
			int temp = end_index - begin_index;
			if (begin_index != end_index && end_index > begin_index) {
				retrieve_title[0] = Text.decode(line.getBytes(), begin_index,
						temp);
			} 
			else
				retrieve_title[0] = " ";		
		} 
		else {
			retrieve_title[0] = " ";
		}
		// Retrieving data between <text> and </text> tags
		if (lineString.contains("<text") && lineString.contains("</text>")) {
			int begin_text = line.find("<text");
			begin_text=line.find(">");
			int end_text = line.find("</text>", begin_text);
			begin_text=begin_text+1;
			int temp = end_text - begin_text;
			if (begin_text != end_text && end_text > begin_text)
				retrieve_title[1] = Text.decode(line.getBytes(), begin_text,
						temp);
			else
				retrieve_title[1] = " ";
		}
		
		else {
			retrieve_title[1] = " ";
		}
		return retrieve_title;
	}
}