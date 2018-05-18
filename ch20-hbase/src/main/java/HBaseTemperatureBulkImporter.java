import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Uses HBase's bulk load facility ({@link HFileOutputFormat2} and {@link
 * LoadIncrementalHFiles}) to efficiently load temperature data into a HBase table.
 */
public class HBaseTemperatureBulkImporter extends Configured implements Tool {
    private static final String TEMPORARY_PATH = "/tmp/bulk";

    static class HBaseTemperatureMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        private NcdcRecordParser parser = new NcdcRecordParser();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            parser.parse(value.toString());
            if (parser.isValidTemperature()) {
                byte[] rowKey = RowKeyConverter.makeObservationRowKey(parser.getStationId(), parser
                        .getObservationDate()
                        .getTime());
                Put put = new Put(rowKey);
                put.addColumn(Bytes.toBytes(HBaseTemperatureQuery.COLUMNFAMILY_DATA), Bytes.toBytes(
                        HBaseTemperatureQuery.QUALIFIER_AIRTEMP), Bytes.toBytes(parser.getAirTemperature()));
                context.write(new ImmutableBytesWritable(rowKey), put);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: HBaseTemperatureBulkImporter <input>");
            return -1;
        }
        Configuration configuration = HBaseConfiguration.create();
        Job job = Job.getInstance(configuration, getClass().getSimpleName());
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path temporaryPath = new Path(TEMPORARY_PATH);
        FileOutputFormat.setOutputPath(job, temporaryPath);
        job.setMapperClass(HBaseTemperatureMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        Connection connection = ConnectionFactory.createConnection(configuration);
        try {
            Admin admin = connection.getAdmin();
            try {
                TableName tableName = TableName.valueOf(HBaseTemperatureQuery.TABLE_NAME);
                Table table = connection.getTable(tableName);
                try {
                    RegionLocator regionLocator = connection.getRegionLocator(tableName);
                    HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

                    if (!job.waitForCompletion(true)) {
                        return 1;
                    }

                    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(configuration);
                    loader.doBulkLoad(temporaryPath, admin, table, regionLocator);
                    FileSystem.get(configuration).delete(temporaryPath, true);
                } finally {
                    table.close();
                }
            } finally {
                admin.close();
            }
        } finally {
            connection.close();
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HBaseTemperatureBulkImporter(), args);
        System.exit(exitCode);
    }
}