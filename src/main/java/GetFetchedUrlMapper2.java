/**
 * Created by zhufangze on 2017/6/2.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Mapper for select
public class GetFetchedUrlMapper2
        extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    private String SELECT_FLAG = "SELECT";

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter("user_mapper2", "TOTAL").increment(1L);

        try {
            String parts[] = value.toString().split("\t");
            if (parts.length != 2) {
                context.getCounter("user_mapper2", "INVALID_LINE").increment(1L);
            }
            String url = parts[1].trim();
            String root_sitemap = parts[0].trim();
            k.set(url);
            v.set(SELECT_FLAG+"\t"+root_sitemap); // "SELECT    root_sitemap"
            context.write(k, v);
        }
        catch(Exception e) {
            context.getCounter("user_mapper2", "EXCEPTION").increment(1L);
        }
    }
}