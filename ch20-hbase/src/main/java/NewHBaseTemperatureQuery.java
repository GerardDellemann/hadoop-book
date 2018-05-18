import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HBase 1.0 version of HBaseTemperatureQuery that uses {@code Connection},
 * and {@code Table}.
 */
public class NewHBaseTemperatureQuery extends Configured implements Tool {
    private static final String TABLE_NAME = "observations";
    private static final String COLUMNFAMILY_DATA = "data";
    private static final String QUALIFIER_AIRTEMP = "airtemp";

    public NavigableMap<Long, Integer> getStationObservations(Table table,
            String stationId, long maxStamp, int maxCount) throws IOException {
        byte[] startRow = RowKeyConverter.makeObservationRowKey(stationId, maxStamp);
        NavigableMap<Long, Integer> resultMap = new TreeMap<Long, Integer>();
        Scan scan = new Scan().withStartRow(startRow);
        scan.addColumn(Bytes.toBytes(COLUMNFAMILY_DATA), Bytes.toBytes(QUALIFIER_AIRTEMP));
        ResultScanner scanner = table.getScanner(scan);
        try {
            Result result;
            int count = 0;
            while ((result = scanner.next()) != null && count++ < maxCount) {
                byte[] row = result.getRow();
                byte[] value = result.getValue(Bytes.toBytes(COLUMNFAMILY_DATA), Bytes.toBytes(QUALIFIER_AIRTEMP));
                Long timestamp = Long.MAX_VALUE -
                        Bytes.toLong(row, row.length - Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
                Integer temperature = Bytes.toInt(value);
                resultMap.put(timestamp, temperature);
            }
        } finally {
            scanner.close();
        }
        return resultMap;
    }

    public int run(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: HBaseTemperatureQuery <station_id>");
            return -1;
        }

        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        try {
            TableName tableName = TableName.valueOf(TABLE_NAME);
            Table table = connection.getTable(tableName);
            try {
                NavigableMap<Long, Integer> observations =
                        getStationObservations(table, args[0], Long.MAX_VALUE, 10).descendingMap();
                for (Map.Entry<Long, Integer> observation : observations.entrySet()) {
                    // Print the date, time, and temperature
                    System.out.printf("%1$tF %1$tR\t%2$s\n", observation.getKey(),
                            observation.getValue());
                }
                return 0;
            } finally {
                table.close();
            }
        } finally {
            connection.close();
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(),
                new NewHBaseTemperatureQuery(), args);
        System.exit(exitCode);
    }
}
