import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HBase 1.0 version of HBaseStationQuery that uses {@code Connection},
 * and {@code Table}.
 */
public class NewHBaseStationQuery extends Configured implements Tool {
    public static final String TABLE_NAME = "stations";
    public static final String COLUMNFAMILY_INFO = "info";
    public static final String QUALIFIER_NAME = "name";
    public static final String QUALIFIER_LOCATION = "location";
    public static final String QUALIFIER_DESCRIPTION = "description";

    public Map<String, String> getStationInfo(Table table, String stationId)
            throws IOException {
        Get get = new Get(Bytes.toBytes(stationId));
        get.addFamily(INFO_COLUMNFAMILY);
        Result result = table.get(get);
        if (result == null) {
            return null;
        }
        Map<String, String> resultMap = new LinkedHashMap<String, String>();
        resultMap.put(NAME_QUALIFIER, getValue(result, Bytes.toBytes(INFO_COLUMNFAMILY), Bytes.toBytes(NAME_QUALIFIER)));
        resultMap.put(LOCATION_QUALIFIER, getValue(result, Bytes.toBytes(INFO_COLUMNFAMILY),
                Bytes.toBytes(LOCATION_QUALIFIER)));
        resultMap.put(DESCRIPTION_QUALIFIER, getValue(result, Bytes.toBytes(INFO_COLUMNFAMILY),
                Bytes.toBytes(DESCRIPTION_QUALIFIER)));
        return resultMap;
    }

    private static String getValue(Result result, byte[] columnFamily, byte[] qualifier) {
        byte[] value = result.getValue(columnFamily, qualifier);
        return value == null ? "" : Bytes.toString(value);
    }

    public int run(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: HBaseStationQuery <station_id>");
            return -1;
        }

        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        try {
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            try {
                Map<String, String> stationInfo = getStationInfo(table, args[0]);
                if (stationInfo == null) {
                    System.err.printf("Station ID %s not found.\n", args[0]);
                    return -1;
                }
                for (Map.Entry<String, String> station : stationInfo.entrySet()) {
                    System.out.printf("%s\t%s\n", station.getKey(), station.getValue());
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
        int exitCode = ToolRunner.run(new NewHBaseStationQuery(), args);
        System.exit(exitCode);
    }
}