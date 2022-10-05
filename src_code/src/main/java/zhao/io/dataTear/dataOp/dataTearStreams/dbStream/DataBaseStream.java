package zhao.io.dataTear.dataOp.dataTearStreams.dbStream;

import zhao.io.dataTear.dataOp.dataTearRW.Reader;
import zhao.io.dataTear.dataOp.dataTearRW.dataBase.DataBaseReader;
import zhao.io.dataTear.dataOp.dataTearRW.dataBase.DataBaseWriter;
import zhao.io.dataTear.dataOp.dataTearStreams.DT_StreamBase;
import zhao.io.ex.ZHAOLackOfInformation;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;

/**
 * 数据库操作集成流，自身是算法库的一员
 * 为了提高连接复用性，连接器不会被框架操作，框架仅仅是使用
 * <p>
 * The database operation integration flow itself is a member of the algorithm library. In order to improve the connection reusability, the connector will not be operated by the framework, and the framework only uses
 *
 * <p>
 * 注意：被操作数据库需要符合JDBC协议
 * <p>
 * Note: The database to be operated must conform to the JDBC protocol
 * <p>
 * 经过测试可对接的数据库 MySQL HIVE 其它数据库不代表不支持，只是还未进行测试
 * <p>
 * Tested and operational databases MySQL HIVE Other databases do not mean that they are not supported, but they have not been tested
 */
public class DataBaseStream implements DT_StreamBase {
    /**
     * 数据库连接对象，这个连接对象需要由您去获取到并设置进来
     * <p>
     * Database connection object, this connection object needs to be obtained and set by you
     */
    Connection connection;
    /**
     * 数据库字段描述，在您使用数据读取流的时候，这里不应该包含字段类型，具体的您可以去参阅文档"Data Tear reads and writes in database"
     * <p>
     * Database field description, when you use the data read stream, the field type should not be included here. For details, you can refer to the document "Data Tear reads and writes in database".
     */
    String[] Schemas;

    /**
     * 数据sql查询时额外条件，这里就是where子句
     * <p>
     * Additional conditions for data sql query, here is the where clause
     */
    String where = "";

    public DataBaseStream() {
        logger1.warn("注意：数据库操作流中无法使用流数量的估算算法，因为不同数据库的数据量信息的获取是不同的，这里采取的是单元格的计数作为读取的数量。");
    }

    /**
     * 设置数据库的连接对象，注意这个连接对象不会被关闭，这样您就可以多次使用该连接对象了
     * <p>
     * Set the connection object of the database, note that this connection object will not be closed, so you can use the connection object multiple times
     *
     * @param connection 数据库的连接对象  database connection object
     * @return 链
     */
    public DataBaseStream setConnection(Connection connection) {
        this.connection = connection;
        return this;
    }

    /**
     * 设置数据查询条件，其中就是where子句，不需要将where添加进去，在调用readStream的时候，该方法会生效
     * <p>
     * Set the data query conditions, which is the where clause, you do not need to add where, when calling read Stream, the method will take effect
     *
     * @param where 查询数据时需要的where条件 注意 针对nameManager 该方法不会去执行where子句
     *              <p>
     *              Where conditions required when querying data Note that this method will not execute the where clause for nameManager
     * @return 链
     */
    public DataBaseStream where(String where) {
        this.where = where;
        return this;
    }

    /**
     * <h3>中文</h3>
     * 设置数据表中的字段们
     * <p>
     * 写数据时 每一个参数都需要字段名以及数据类型描述
     * <p>
     * 示例：setSchemas("c1 varchar(30)", "c2 varchar(30)", "c3 varchar(30)", "c4 varchar(30)", "c5 varchar(30)", "c6 varchar(30)", "c7 varchar(30)"....)
     * <p>
     * 读数据时 每一个参数只需要字段名称就可以了
     * <p>
     * 示例：setSchemas("c1", "c2", "c3"....)
     * <h3>English</h3>
     * Set the fields in the data table <p> Each parameter requires a field name and a data type description when writing data<p> Example: setSchemas("c1 varchar(30)", "c2 varchar(30)", "c3 varchar (30)", "c4 varchar(30)", "c5 varchar(30)", "c6 varchar(30)", "c7 varchar(30)"....) <p> Each parameter when reading data All you need is the field name <p> Example: setSchemas("c1", "c2", "c3"....)
     *
     * @param schemas 表字段
     * @return 链
     */
    public DataBaseStream setSchemas(String... schemas) {
        Schemas = schemas;
        return this;
    }

    /**
     * 解析数据表名称的函数，防止数据库中出现表名错误，因为DataTear协议与数据库中的由冲突，该方法可以将冲突消除
     * <p>
     * The function of parsing the data table name to prevent table name errors in the database, because the Data Tear protocol conflicts with the data table in the database, this method can eliminate the conflict
     *
     * @param tableName 数据表名称
     * @return 解析之后的数据表名称
     * <p>
     * Data table name after parsing
     */
    private String toSQLTable(String tableName) {
        StringBuilder NEW_TableName = new StringBuilder();
        char[] chars = tableName.toCharArray();
        for (char nowChar : chars) {
            if (nowChar != '.' && nowChar != '-' && nowChar != '/') {
                NEW_TableName.append(nowChar);
            } else if (nowChar == '/') {
                NEW_TableName.append(".");
            } else if (nowChar == '-') {
                NEW_TableName.append("_");
            } else {
                break;
            }
        }
        return NEW_TableName.toString();
    }

    /**
     * 解析数据输入表名称，同时将表的读取流打开准备读取数据
     * <p>
     * Parse the data input table name, and open the read stream of the table to read the data
     *
     * @param tableName 数据表名称 注意 表格式为："xxx.NameManager.xxx" 将会被视为NameManager，直接采取API的setInFilePath的设置参数
     *                  <p>
     *                  Data table name Note that the table format is: "xxx.NameManager.xxx" will be regarded as NameManager, directly take the setting parameters of API's setInFilePath
     * @return 数据库表的read组件
     * <p>
     * read component of database table
     * @throws IOException 操作流打开异常
     *                     <p>
     *                     Operation stream open exception
     */
    @Override
    public Reader readStream(String tableName) throws IOException {
        boolean isNameManager = tableName.split("\\.")[1].equalsIgnoreCase("nameManager");
        try {
            return DataBaseReader.builder().setConnection(connection).select(this.Schemas).from(isNameManager ? tableName : toSQLTable(tableName)
            ).where(isNameManager ? "" : where);
        } catch (ZHAOLackOfInformation n) {
            n.printStackTrace(System.err);
            throw new ZHAOLackOfInformation("算法库中的数据库读取操作流有尝试被启用，但是启用失败，可能是由于您的API调用异常，本操作流也是需要强转进行参数设置的。" +
                    "\n示例：((DataBaseStream) RW.getDT_UDF_Stream(DT_builtIn_UDF.SQLDB_TEXT)).setConnection(连接对象).where(\"sex = '男'\").readStream(s);" + n);
        }
    }

    @Override
    public OutputStream writeStream(String OutTable) throws IOException {
        try {
            return DataBaseWriter.builder().Connection(connection).Insert_into(toSQLTable(OutTable), this.Schemas).start();
        } catch (ZHAOLackOfInformation n) {
            n.printStackTrace(System.err);
            throw new ZHAOLackOfInformation("算法库中的数据库读取操作流有尝试被启用，但是启用失败，可能是由于您的API调用异常，本操作流也是需要强转进行参数设置的。" +
                    "\n示例：((DataBaseStream) RW.getDT_UDF_Stream(DT_builtIn_UDF.SQLDB_TEXT)).setConnection(连对象).setSchemas(\"xxx\", \"xxx\", \"xxx\").writeStream(s)" + n);
        }
    }
}
