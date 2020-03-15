/**
 * Created by linfeng on 2020/01/01.
 */

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class DBOutputWritable implements Writable, DBWritable {
    private String datetime;
    private String key1;
    private String key2;
    private String val1;
    private String val2;

    public DBOutputWritable() {

    }

    public DBOutputWritable(String datetime, String key1, String key2, String val1, String val2) {
        this.datetime = datetime;
        this.key1 = key1;
        this.key2 = key2;
        this.val1 = val1;
        this.val2 = val2;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.datetime = dataInput.readUTF();
        this.key1 = dataInput.readUTF();
        this.key2 = dataInput.readUTF();
        this.val1 = dataInput.readUTF();
        this.val2 = dataInput.readUTF();
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.datetime = resultSet.getString("datetime");
        this.key1 = resultSet.getString("key1");
        this.key2 = resultSet.getString("key2");
        this.val1 = resultSet.getString("val1");
        this.val2 = resultSet.getString("val2");
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.datetime);
        dataOutput.writeUTF(this.key1);
        dataOutput.writeUTF(this.key2);
        dataOutput.writeUTF(this.val1);
        dataOutput.writeUTF(this.val2);
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, this.datetime);
        preparedStatement.setString(2, this.key1);
        preparedStatement.setString(3, this.key2);
        preparedStatement.setString(4, this.val1);
        preparedStatement.setString(5, this.val2);
    }
}
