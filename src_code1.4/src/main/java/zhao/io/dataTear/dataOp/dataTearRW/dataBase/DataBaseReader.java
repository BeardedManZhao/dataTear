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
 * 从数据库中读取DT数据库的组件，被读取的数据库需要是符合JDBC协议的。
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
     * 开始建造本组件
     *
     * @return 链
     */
    public static DataBaseReader builder() {
        return new DataBaseReader();
    }

    /**
     * 查询列设置，如果不设置将代表所有列都查询
     *
     * @param select 需要被查询的列
     * @return 链
     */
    public DataBaseReader select(String... select) {
        this.select = Arrays.stream(select).reduce((x, y) -> x + ", " + y).orElse("*");
        return this;
    }

    /**
     * @return 本DT文件的源文件名称
     */
    @Override
    public String getSrcFile() {
        return preparedStatement.toString();
    }

    /**
     * @param srcFile 为子类提供的源文件路径设置方法
     */
    @Override
    protected void setSrcFile(String srcFile) {
        super.setSrcFile(srcFile);
    }

    /**
     * 将读取到的数据加载到内存的方法，由内部调用
     *
     * @param datas Byte数组
     */
    @Override
    protected void setByteArray(byte[] datas) {
        super.setByteArray(datas);
    }

    /**
     * @return 数据库中被读取表的一些信息
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
     * 来源于接口的调用
     *
     * @param in_FilePath 这里是来源于哪个表
     * @return MySQL数据读取组件
     * @deprecated 不建议使用，可以直接使用from方法, 因为该方法与from中的实现是一致的，同时这个方法不允许使用where等处理计算
     */
    @Override
    @Deprecated
    public Reader setIn_FilePath(String in_FilePath) {
        return from(in_FilePath);
    }

    /**
     * @return Reader中包含的数据输入流 如果有设置过输入流的话
     * @deprecated 本组件中不需要使用任何的外界数据流
     */
    @Override
    @Deprecated
    public InputStream getInputStream() {
        return super.getInputStream();
    }

    /**
     * @param inputStream 数据输入流设置
     * @return 链
     * @deprecated 本组件中不需要使用任何的外界数据流
     */
    @Override
    @Deprecated
    public Reader setInputStream(InputStream inputStream) {
        return super.setInputStream(inputStream);
    }

    /**
     * @return 本组件获取到的数据byte数组
     */
    @Override
    public byte[] getDataArray() {
        return super.getDataArray();
    }

    /**
     * @return 本组件获取到的数据String形式返回
     */
    @Override
    public String getDataString() {
        return super.getDataString();
    }

    /**
     * @return 文件对象 失效的文件对象
     * @deprecated 针对数据库连接，不需要使用组件的方式进行读取
     */
    @Override
    @Deprecated
    public File getIn_File() {
        return new File(TableName);
    }

    /**
     * @param in_File 文件对象
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
     * 注意 不会关闭connect对象，由调用者决定connect是否关闭
     *
     * @return 是否关闭成功
     * @throws IOException 关闭失败可能抛出的异常
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
     */
    @Override
    public int available() {
        return columnCount * RowCount;
    }

    /**
     * 设置数据来源于哪个表
     *
     * @param TableName 表名称
     * @return 链
     */
    public DataBaseReader from(String TableName) {
        this.TableName = TableName;
        return this;
    }

    /**
     * 设置数据库连接组件，便于连接数据库
     *
     * @param connection 数据库连接组件
     * @return 链
     */
    public DataBaseReader setConnection(Connection connection) {
        this.connection = connection;
        return this;
    }

    /**
     * 设置数据查询SQL额外语句
     *
     * @param where where 关键字后面的语句，这里不需要带有where
     *              例如：name = 'zhao' group by sex
     * @return 链
     */
    public DataBaseReader where(String where) {
        this.where = where;
        return this;
    }

}
