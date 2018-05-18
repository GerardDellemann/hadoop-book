import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class DeleteData {
    private static final String TABLE_NAME = "ietsanders2";
    private static final String COLUMN_FAMILY_NAME = "data";
    private static final String QUALIFIER_NAME = "1";
    private static final String ROW_NAME = "row1";

    public static void main(String[] args) throws IOException {
        // Open connection
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        try {
            // Instantiating HTable class
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

            try {
                // Instantiating Delete class
                Delete delete = new Delete(Bytes.toBytes(ROW_NAME));
                delete.addColumn(Bytes.toBytes(COLUMN_FAMILY_NAME), Bytes.toBytes(QUALIFIER_NAME));
                delete.addFamily(Bytes.toBytes(COLUMN_FAMILY_NAME));

                // deleting the data
                table.delete(delete);
                System.out.println("data deleted.....");
            } finally {
                // closing the HTable object
                table.close();
            }
        } finally {
            connection.close();
        }
    }
}