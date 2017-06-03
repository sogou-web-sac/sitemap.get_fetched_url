/**
 * Created by zhufangze on 2017/6/2.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Mapper for fetch feedback
public class GetFetchedUrlMapper1
        extends Mapper<LongWritable, Text, Text, Text> {

    private Text k = new Text();
    private Text v = new Text();

    private String UNKNOWN = "unknown";
    private String WAP_SITEMAP_BATCH_ID = "wap_sitemap";
    private String SUCCESS = "SUCCESS";
    private String FETCH_FLAG = "FETCH";

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.getCounter("user_mapper1", "TOTAL").increment(1L);

        try {
            String line = value.toString();
            if (line.trim().length() == 0) {
                context.getCounter("user_mapper1", "INVALID_LINE").increment(1L);
                return;
            }

            String url = null;
            String SITEMAP_TYPE = context.getConfiguration().get("user.param.sitemap.type", UNKNOWN);

            if (SITEMAP_TYPE.equals("web")) { // feedback of spider
                url = line.split("\t")[0].trim();
            }else if (SITEMAP_TYPE.equals("wap")) { // spider log
                String parts[] = line.split(" ");
                if (parts.length < 5)
                    return;
                if (parts[2].trim().equals(WAP_SITEMAP_BATCH_ID) && parts[3].trim().equals(SUCCESS))
                    url = parts[4].trim();
            }else {
                context.getCounter("user_mapper1", "INVALID_PARAM").increment(1L);
                return;
            }

            k.set(url);
            v.set(FETCH_FLAG);
            context.write(k, v);
        }
        catch(Exception e) {
            context.getCounter("user_mapper1", "EXCEPTION").increment(1L);
        }
    }
}