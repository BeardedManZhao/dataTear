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
 * 为了提高连接复用性，连接器不会被框架操作，仅仅可以使用
 * <p>
 * 注意：被操作数据库需要符合JDBC协议
 * <p>
 * 经过测试可对接的数据库 MySQL HIVE 其它数据库不代表不支持，只是还未进行测试
 */
public class DataBaseStream implements DT_StreamBase {
    /**
     * 数据库连接对象
     */
    Connection connection;
    /**
     * 数据库字段描述
     */
    String[] Schemas;
    /**
     * 数据sql查询时额外条件
     */
    String where = "";

    public DataBaseStream() {
        logger1.warn("注意：数据库操作流中无法使用流数量的估算算法，因为不同数据库的数据量信息的获取是不同的，这里采取的是单元格的计数作为读取的数量。");
    }

    /**
     * 设置数据库的连接对象
     *
     * @param connection 数据库的连接对象
     * @return 链
     */
    public DataBaseStream setConnection(Connection connection) {
        this.connection = connection;
        return this;
    }

    /**
     * 设置数据查询条件
     *
     * @param where 查询数据时需要的where条件 注意 针对nameManager 该方法不会去执行where子句
     * @return 链
     */
    public DataBaseStream where(String where) {
        this.where = where;
        return this;
    }

    /**
     * 设置数据表中的字段们
     * <p>
     * 写数据时 每一个参数都需要字段名以及数据类型描述
     * <p>
     * 示例：setSchemas("c1 varchar(30)", "c2 varchar(30)", "c3 varchar(30)", "c4 varchar(30)", "c5 varchar(30)", "c6 varchar(30)", "c7 varchar(30)"....)
     * <p>
     * 读数据时 每一个参数只需要字段名称就可以了
     * <p>
     * 示例：setSchemas("c1", "c2", "c3"....)
     *
     * @param schemas 表字段
     * @return 链
     */
    public DataBaseStream setSchemas(String... schemas) {
        Schemas = schemas;
        return this;
    }

    /**
     * 解析数据表名称的函数，方式数据库中出现表名错误
     *
     * @param tableName 数据表名称
     * @return 解析之后的数据表名称
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
     *
     * @param tableName 数据表名称 注意 表格式为：xxx.NameManager.xxx 将会被视为NameManager，直接采取API的setInFilePath的设置参数
     * @return 数据库表的read组件
     * @throws IOException 操作流打开异常
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
