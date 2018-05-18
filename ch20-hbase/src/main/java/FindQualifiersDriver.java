import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FindQualifiersDriver extends Configured implements Tool {
    // Tabel
    private static final String TABLE_NAME = "ietsanders2";
    private static final String COLUMN_FAMILY_NAME = "data";

    // Job
    private static final String JOB_NAME = "Distinct_columns";
    private static final String OUTPUT_PATH = "output/";

    static class OnlyColumnNameMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, final Context context) throws IOException,
                InterruptedException {
            CellScanner cellScanner = value.cellScanner();
            while (cellScanner.advance()) {

                Cell cell = cellScanner.current();
                byte[] qualifier = Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(), cell
                        .getQualifierLength());

                context.write(new Text(qualifier), new Text());
            }
        }
    }

    static class OnlyColumnNameReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            context.write(new Text(key), new Text());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Path outputPath = new Path(OUTPUT_PATH);
        String tableName = new String(TABLE_NAME);
        byte[] columnFamilyName = new String(COLUMN_FAMILY_NAME).getBytes();

        Configuration configuration = HBaseConfiguration.create();
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.delete(outputPath, true);

        Job job = Job.getInstance(configuration, JOB_NAME);
        job.setJarByClass(this.getClass());

        Scan scan = new Scan();
        scan.setBatch(500);
        scan.addFamily(columnFamilyName);
        scan.setFilter(new KeyOnlyFilter()); // scan only key part of KeyValue (raw, column family, column)
        scan.setCacheBlocks(false); // don't set to true for MR jobs

        TextOutputFormat.setOutputPath(job, outputPath);

        TableMapReduceUtil.initTableMapperJob(tableName, scan, OnlyColumnNameMapper.class, // mapper
                Text.class, // mapper output key
                Text.class, // mapper output value
                job);

        job.setNumReduceTasks(1);
        job.setReducerClass(OnlyColumnNameReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new FindQualifiersDriver(), args);
        System.exit(exitCode);
    }
}
