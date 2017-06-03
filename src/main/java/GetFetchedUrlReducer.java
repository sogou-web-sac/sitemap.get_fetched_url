/**
 * Created by zhufangze on 2017/6/2.
 */
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GetFetchedUrlReducer
        extends Reducer<Text, Text, Text, Text> {

    Text k = new Text();
    Text v = new Text();

    private String FETCH_FLAG = "FETCH";
    private String SELECT_FLAG = "SELECT";

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        context.getCounter("user_reducer", "TOTAL").increment(1L);

        try {
            boolean is_from_mapper1 = false;
            boolean is_from_mapper2 = false;

            String root_sitemap = null;
            for (Text i : values) {
                String from_where = i.toString();
                if (from_where.indexOf(FETCH_FLAG) != -1) {
                    is_from_mapper1 = true;
                }
                else if (from_where.indexOf(SELECT_FLAG) != -1) {
                    is_from_mapper2 = true;
                    root_sitemap = from_where.split("\t")[1].trim();
                }
            }

            if (is_from_mapper1 && is_from_mapper2) {
                String url = key.toString();
                k.set(root_sitemap);
                v.set(url);
                context.write(k, v); // ROOT_SITEMAP\turl
                context.getCounter("user_reducer", "OUTPUT").increment(1L);
            }
        }
        catch (Exception e) {
            context.getCounter("user_reducer", "Exception").increment(1L);
        }
    }
}