import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder.ModifyableTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

public class ExampleClient {
    private static final String TABLE_NAME = "nieuwetest";
    private static final String COLUMN_FAMILY_NAME = "data";
    private static final String ROW_BASE_NAME = "row";
    private static final String VALUE_BASE_NAME = "value";

    public static void main(String[] args) throws IOException {
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        try {
            Admin admin = connection.getAdmin();
            try {
                // Create table
                TableName tableName = TableName.valueOf(TABLE_NAME);
                ModifyableTableDescriptor tableDescriptor = (ModifyableTableDescriptor) TableDescriptorBuilder
                        .newBuilder(tableName)
                        .build();
                ModifyableColumnFamilyDescriptor columnFamilyDescriptor =
                        (ModifyableColumnFamilyDescriptor) ColumnFamilyDescriptorBuilder
                                .newBuilder(Bytes.toBytes(COLUMN_FAMILY_NAME))
                                .build();
                tableDescriptor.setColumnFamily(columnFamilyDescriptor);
                admin.createTable(tableDescriptor);
                // Run some operations -- three puts, a get, and a scan -- against the table.
                Table table = connection.getTable(tableName);
                try {
                    for (int i = 1; i <= 3; i++) {
                        byte[] row = Bytes.toBytes(ROW_BASE_NAME + i);
                        Put put = new Put(row);
                        byte[] columnFamily = Bytes.toBytes(COLUMN_FAMILY_NAME);
                        byte[] qualifier = Bytes.toBytes(String.valueOf(i));
                        byte[] value = Bytes.toBytes(VALUE_BASE_NAME + i);
                        put.addColumn(columnFamily, qualifier, value);
                        table.put(put);
                    }
                    Get get = new Get(Bytes.toBytes(ROW_BASE_NAME + "1"));
                    Result result = table.get(get);
                    System.out.println("Get: " + result);
                    Scan scan = new Scan();
                    ResultScanner scanner = table.getScanner(scan);
                    try {
                        for (Result scannerResult : scanner) {
                            System.out.println("Scan: " + scannerResult);
                        }
                    } finally {
                        scanner.close();
                    }
                } finally {
                    table.close();
                }
                // Disable then drop the table
                // admin.disableTable(tableName);
                // admin.deleteTable(tableName);
            } finally {
                admin.close();
            }
        } finally {
            connection.close();
        }
    }
}