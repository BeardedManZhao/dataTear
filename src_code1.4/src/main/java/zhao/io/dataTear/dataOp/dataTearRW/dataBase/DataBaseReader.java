package zhao.io.dataTear.dataOp.dataTearRW.dataBase;

import zhao.io.dataTear.dataOp.dataTearRW.Reader;
import zhao.io.ex.ZHAOLackOfInformation;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.Arrays;

/**
 * @author 赵凌宇
 * 从数据库中读取DT数据库的组件，被读取的数据库需要是符合JDBC协议的，实现了使用DT的方式将数据读取进来的操作
 * <p>
 * The component that reads the DT database from the database, the database to be read needs to conform to the JDBC protocol, and the operation of reading data in the way of DT is realized.
 */
public class DataBaseReader extends Reader {
    String TableName;
    boolean isNameManager = false;
    private String select = "*";
    private String where = "";
    private Connection connection;
    private PreparedStatement preparedStatement;
    private int columnCount;
    private int RowCount;

    /**
     * 开始建造本组件，这个方法是开始own object, which can be used for chain programming式设置数据读取组件的第一步
     * <p>
     * Start building this component, this method is the first step to start chaining setup data read components.
     *
     * @return own object, which can be used for chain programming
     */
    public static DataBaseReader builder() {
        return new DataBaseReader();
    }

    /**
     * 查询列设置，如果不设置将代表所有列都查询。
     * <p>
     * Query column settings, if not set, all columns will be queried.
     *
     * @param select 需要被查询的列  Columns to be queried
     * @return own object, which can be used for chain programming
     */
    public DataBaseReader select(String... select) {
        this.select = Arrays.stream(select).reduce((x, y) -> x + ", " + y).orElse("*");
        return this;
    }

    /**
     * @return 本DT文件的源文件名称
     * <p>
     * The source file name of this DT file
     */
    @Override
    public String getSrcFile() {
        return preparedStatement.toString();
    }

    /**
     * @param srcFile 为子类提供的源文件路径设置方法
     *                <p>
     *                Set method for source file path provided by subclass
     */
    @Override
    protected void setSrcFile(String srcFile) {
        super.setSrcFile(srcFile);
    }

    /**
     * 将读取到的数据加载到内存的方法，由内部调用。
     * <p>
     * The method for loading the read data into memory, which is called internally.
     *
     * @param datas Byte数组，是数据的byte数组形式
     *              <p>
     *              Byte array, which is the byte array form of the data
     */
    @Override
    protected void setByteArray(byte[] datas) {
        super.setByteArray(datas);
    }

    /**
     * @return 数据库中被读取表的一些信息，同样返回的还是源信息，只是这个的源就是数据库
     * <p>
     * Some information of the read table in the database also returns the source information, but the source of this is the database.
     */
    @Override
    public String getIn_FilePath() {
        try {
            return "DataBase://" + InetAddress.getLocalHost().getHostAddress() + "/" + this.TableName;
        } catch (UnknownHostException e) {
            return "DataBase://" + "--------/" + this.TableName;
        }
    }

    /**
     * 设置读取的表信息，和from 方法效果相同，设置组件读取的数据表。
     * <p>
     * Set the read table information, which has the same effect as the "from" method, setting the data table read by the component.
     *
     * @param in_FilePath 数据是来源于哪个表，这个参数就是表名。
     *                    <p>
     *                    Which table the data comes from, this parameter is the table name.
     * @return own object, which can be used for chain programming
     * @deprecated 不建议使用，可以直接使用from方法, 因为该方法与from中的实现是一致的，同时这个方法不允许使用where等处理计算
     * <p>
     * It is recommended to use the "from" method directly, because this method is consistent with the implementation in "from", and this method does not allow processing calculations such as where.
     */
    @Override
    @Deprecated
    public Reader setIn_FilePath(String in_FilePath) {
        return from(in_FilePath);
    }

    /**
     * @return 如果有设置过输入流的话，返回Reader中包含的数据输入流，反之可能为null。
     * <p>
     * If the input stream has been set, return the data input stream contained in the Reader, otherwise it may be null.
     * @deprecated 本组件中不需要使用任何的外界数据流。
     * <p>
     * This component does not need to use any external data flow.
     */
    @Override
    @Deprecated
    public InputStream getInputStream() {
        return super.getInputStream();
    }

    /**
     * 设置数据输入组件，注意，本组件自身就实现了数据流功能，不需要向该组件中传入数据流。
     * <p>
     * Set the data input component. Note that this component itself implements the data flow function, and there is no need to pass data flow to this component.
     *
     * @param inputStream 数据输入流设置  Data input stream settings
     * @return own object, which can be used for chain programming
     * @deprecated 本组件中不需要使用任何的外界数据流，您即使设置了也不会被调用。
     * <p>
     * This component does not need to use any external data flow, even if you set it, it will not be called.
     */
    @Override
    @Deprecated
    public Reader setInputStream(InputStream inputStream) {
        return super.setInputStream(inputStream);
    }

    /**
     * @return 本组件获取到的数据byte数组，注意，数据碎片的分割符是”,“，NM的分隔符是”=“。
     * <p>
     * The data byte array obtained by this component, note that the delimiter of the data fragment is ",", and the delimiter of NM is "=".
     */
    @Override
    public byte[] getDataArray() {
        return super.getDataArray();
    }

    /**
     * @return 本组件获取到的数据，以"String"的数据类型返回数据，注意，数据碎片的分割符是”,“，NM的分隔符是”=“。
     * <p>
     * The data obtained by this component is returned to the data type of "String". Note that the delimiter of the data fragment is ",", and the delimiter of NM is "=".
     */
    @Override
    public String getDataString() {
        return super.getDataString();
    }

    /**
     * @return 文件对象 失效的文件对象 file object invalid file object
     * @deprecated 针对数据库连接，不需要使用组件的方式进行读取
     * <p>
     * For database connections, there is no need to use components to read
     */
    @Override
    @Deprecated
    public File getIn_File() {
        return new File(TableName);
    }

    /**
     * @param in_File 数据输入文件对象 您不需要对该方法进行设置
     * @return 文件对象 失效的文件对象
     * @deprecated 针对数据库连接，不需要使用组件的方式进行读取
     */
    @Override
    @Deprecated
    public Reader setIn_File(File in_File) {
        logger.warn("针对数据库的读取，您不需要使用File文件对象。因此该方法将不会有任何的操作。");
        return this;
    }

    @Override
    public boolean openStream() {
        try {
            isNameManager = TableName.split("\\.")[1].equalsIgnoreCase("nameManager");
        } catch (ArrayIndexOutOfBoundsException | NullPointerException a) {
            logger.error("请检查您的表设置是否正确，有尝试解析输入设置，但是解析失败。");
            throw new ZHAOLackOfInformation("DataBaseReader组件尝试解析表[" + TableName + "]但是解析失败！");
        }
        try {
            logger.info("开始打开对于表的数据读取操作流，目标 => " + TableName);
            preparedStatement = connection.prepareStatement(
                    "select " + (isNameManager ? "*" : select) + " from " + TableName + (where.length() == 0 ? ";" : " where " + where + ";")
            );
            ResultSetMetaData metaData = preparedStatement.getMetaData();
            columnCount = metaData.getColumnCount();
            Statement getRowCount = connection.createStatement();
            ResultSet RowCount_resultSet = getRowCount.executeQuery("select count(*) from " + TableName);
            RowCount_resultSet.next();
            RowCount = RowCount_resultSet.getInt(1);
            getRowCount.close();
            RowCount_resultSet.close();
        } catch (NullPointerException | SQLException e) {
            e.printStackTrace(System.err);
            String error = "您的DataBaseReader组件数据流有尝试打开，但是出现了异常，请您检查该组件的API调用" +
                    "\n\t单数据流实例化示例：DataBaseReader.builder().setConnection(连接对象).from(\"table\").where(\"a = 123\")" +
                    "\n\t算法库拉取API示例：return ((DataBaseStream) RW.getDT_UDF_Stream(DT_builtIn_UDF.SQLDB_TEXT)).setConnection(连接对象).where(\"sex = '男'\").readStream(s);";
            logger.error(error);
            throw new ZHAOLackOfInformation(error);
        }
        return true;
    }

    @Override
    public boolean op_Data() {
        // 注意 切分符号会根据是否为nameManager的布尔改变
        byte[] splitChar = isNameManager ? "=".getBytes() : ",".getBytes();
        byte[] lineChar = "\n".getBytes();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ResultSet resultSet;
        try {
            resultSet = preparedStatement.executeQuery();
            boolean reaultNext = resultSet.next();
            while (reaultNext) {
                // 获取数据库表的每一行
                for (int now = 1; now <= columnCount; now++) {
                    // 获取数据行的每一个单元格，写进数组输出流
                    byteArrayOutputStream.write(resultSet.getBytes(now));
                    if (now != columnCount) byteArrayOutputStream.write(splitChar);
                }
                reaultNext = resultSet.next();
                if (reaultNext) {
                    // 如果不是一个数据表中的最后一行，就使用换行符追加，因为外界没有表概念，换行符需要被添加
                    byteArrayOutputStream.write(lineChar);
                }
            }
        } catch (SQLException | IOException | NullPointerException e) {
            logger.error("数据库数据操作组件，出现了异常，异常原因：" + e);
            e.printStackTrace(System.err);
            return false;
        }
        try {
            resultSet.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            byteArrayOutputStream.flush();
            byteArrayOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        setByteArray(byteArrayOutputStream.toByteArray());
        return true;
    }

    /**
     * 关闭”preparedStatement“注意 不会关闭connect对象，由调用者决定connect是否关闭，这是为了提高连接复用
     * <p>
     * Close "preparedStatement" Note that to connect object will not be closed, the caller decides whether to close to connect, this is to improve the connection multiplex.
     *
     * @return 是否关闭成功  Is it closed successfully
     * @throws IOException 关闭失败可能抛出的异常
     *                     <p>
     *                     Exceptions that may be thrown on shutdown failures
     */
    @Override
    public boolean closeStream() throws IOException {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    /**
     * @return 被查询表的单元格数量
     * The number of cells in the queried table
     */
    @Override
    public int available() {
        return columnCount * RowCount;
    }

    /**
     * 设置数据来源于哪个表，和"SQL"语句中的"from"语法的使用基本一致
     * <p>
     * Set which table the data comes from, which is basically the same as the use of the "from" syntax in the "SQL" statement
     *
     * @param TableName 表名称
     * @return own object, which can be used for chain programming
     */
    public DataBaseReader from(String TableName) {
        this.TableName = TableName;
        return this;
    }

    /**
     * 设置数据库连接组件，便于连接数据库服务器
     * <p>
     * Set up database connection components to facilitate connection to the database server.
     *
     * @param connection 数据库连接组件
     * @return own object, which can be used for chain programming
     */
    public DataBaseReader setConnection(Connection connection) {
        this.connection = connection;
        return this;
    }

    /**
     * 设置数据查询SQL的where子句，不需要包含where本身！
     * <p>
     * Set the where clause of data query SQL without including where itself!
     *
     * @param where where 关键字后面的语句，这里不需要带有where
     *              such as：name = 'zhao' group by sex
     * @return own object, which can be used for chain programming
     */
    public DataBaseReader where(String where) {
        this.where = where;
        return this;
    }

}
