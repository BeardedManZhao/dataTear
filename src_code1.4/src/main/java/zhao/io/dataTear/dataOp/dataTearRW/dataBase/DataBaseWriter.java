package zhao.io.dataTear.dataOp.dataTearRW.dataBase;

import zhao.io.dataTear.dataOp.dataTearRW.Writer;
import zhao.io.ex.OutUDFException;
import zhao.io.ex.ZHAOLackOfInformation;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * @author 赵凌宇
 * 写DT数据库的组件，被操作的数据库需要是符合JDBC协议的。
 */
public class DataBaseWriter extends Writer {

    String TableName;
    String[] schemas;
    boolean isNameManager = true;
    private int columnCount;
    private Connection connection;
    /**
     * 数据库操作对象
     */
    private PreparedStatement preparedStatement;

    /**
     * 开始建造本组件
     *
     * @return 链
     */
    public static DataBaseWriter builder() {
        return new DataBaseWriter();
    }

    /**
     * 设置输出模式，该方法对写数组时产生巨大的作用，构建nameManager与Fragmentation的算法是不同的，因此需要通过此参数进行输出模式的调整
     * <p>
     * 该方法会被Master自动的调用
     *
     * @param isnameManager 是否使用nameManager输出模式
     */
    public void setisNameManager(boolean isnameManager) {
        this.isNameManager = isnameManager;
    }

    /**
     * 连接到哪个数据库，注意 该方法在start之前不会执行
     *
     * @param connection 指定数据库的连接对象
     * @return 链
     */
    public DataBaseWriter Connection(Connection connection) {
        this.connection = connection;
        return this;
    }

    /**
     * 操作哪一个数据表，注意 该方法在start之前不会执行
     *
     * @param tableName 一般来说，可以使用不同数据库支持的不同语法操作，例如MySQL, 数据库.数据表的形式 指定数据表
     * @param schemas   表中的所有字段描述, 需要指定数据类型
     * @return 链
     */
    public DataBaseWriter Insert_into(String tableName, String... schemas) {
        TableName = tableName;
        this.schemas = schemas;
        try {
            isNameManager = TableName.split("\\.")[1].equalsIgnoreCase("nameManager");
        } catch (ArrayIndexOutOfBoundsException | NullPointerException a) {
            logger.error("请检查您的库设置是否正确，有尝试解析输出设置，但是解析失败。");
            throw new ZHAOLackOfInformation("DataBaseReader组件尝试解析数据库[" + TableName + "]但是解析失败！");
        }
        return this;
    }

    /**
     * 启动数据表插入流
     * 获取数据库写数据流 同时解析表数据 并创建出数据表 单字段默认是512字符长度，如果需要自己定制，请提前创建好数据表
     *
     * @return 链
     */
    public DataBaseWriter start() {
        try {
            columnCount = schemas.length;
            Statement createTableStatement = connection.createStatement();
            // 根据是否是nameManager的信息，构建数据表，nameManager的数据表是固定字段数量
            String SQL = isNameManager ?
                    "create table if not exists " + TableName + " (NK varchar(50), NV varchar(512));"
                    :
                    "create table if not exists " + TableName + " (" + Arrays.stream(schemas)
                            .reduce((x, y) -> x + " , " + y).orElse("def varchar(100)") + ");";
            logger.info("构建/追加数据表【" + TableName + "】" + SQL);
            createTableStatement.executeLargeUpdate(SQL);
            createTableStatement.close();
        } catch (SQLException e) {
            logger.error("数据表[" + TableName + "]的创建出现了异常！异常原因：" + e.getSQLState());
            e.printStackTrace(System.err);
            throw new OutUDFException("数据库创建表【" + TableName + "】异常。");
        } catch (NullPointerException n) {
            n.printStackTrace(System.err);
            throw new ZHAOLackOfInformation("数据库写出流尝试操作，但是发生了异常：" + n +
                    "\n\t算法库拉取API示例：return ((DataBaseStream) RW.getDT_UDF_Stream(DT_builtIn_UDF.SQLDB_TEXT)).setConnection(连接对象).setSchemas(\"xxx\", \"xxx\", \"xxx\").writeStream(s)");
        }
        try {
            // 根据是否是nameManager的信息，构建插入语句操作对象
            preparedStatement = isNameManager ?
                    connection.prepareStatement("insert into " + TableName + "(NK, NV) values (?, ?);") :
                    connection.prepareStatement(
                            "insert into " + TableName + " (" +
                                    Arrays.stream(schemas).map(str -> str.split("\\s+")[0]).reduce((x, y) -> x + "," + y).orElse("") + ") values (" +
                                    Arrays.stream(schemas).map(str -> "?").reduce((identity, accumulator) -> identity + ", " + accumulator).orElse("?") + ");"
                    );
        } catch (SQLException e) {
            logger.error("数据表写入组件的创建出现了异常，异常原因：" + e.getSQLState());
            e.printStackTrace(System.err);
            throw new OutUDFException("数据表写入组件的创建出现了异常，异常原因：" + e.getSQLState());
        }
        return this;
    }

    /**
     * @return 有关数据表的一些信息
     */
    @Override
    public String getPath() {
        return "/DataBase/" + TableName;
    }

    /**
     * 批量填充数据，这里是填充一行相同的数值
     *
     * @param b 需要填充的数值
     * @throws IOException 数据写入异常
     */
    @Override
    public void write(int b) throws IOException {
        try {
            for (int cmun = 0; cmun <= this.columnCount; cmun++) {
                preparedStatement.setObject(cmun, b);
            }
            preparedStatement.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Override
    @Deprecated
    public Writer toTobject() {
        return this;
    }

    /**
     * 向数据库中写一张表
     *
     * @param TableData 需要写的行数据们
     * @throws SQLException 数据写入数据表时出现异常！
     */
    public void write(ArrayList<String[]> TableData) throws SQLException {
        // 不是nameManager的输出
        for (String[] lines : TableData) {
            int lineLen = lines.length;
            for (int cn = 0; cn < this.columnCount; cn++) {
                preparedStatement.setObject(cn + 1, cn >= lineLen ? "----" : lines[cn]);
            }
            preparedStatement.addBatch();
        }
    }

    /**
     * 向数据库中写数据，根据表名判断数据是否使用nameManager的输出模式, 需要使用逗号做列分割哦
     * <p>
     * 注意 如果是包含换行符，将导致按行写数据
     *
     * @param b 一行数据的byte形式
     * @throws IOException 写数据异常
     */
    @Override
    public void write(byte[] b) throws IOException {
        String linestr = new String(b).trim();
        try {
            if (!isNameManager) {
                // 不是nameManager的输出
                ArrayList<String[]> lines = Arrays.stream(linestr.split("\n+")).map(line -> line.split(",")).collect(Collectors.toCollection(ArrayList::new));
                for (String[] cells : lines) {
                    int cellLen = cells.length;
                    for (int c = 0b0; c < this.columnCount; c++) {
                        preparedStatement.setObject(c + 1, c >= cellLen ? "----" : cells[c]);
                    }
                    preparedStatement.addBatch();
                }
            } else {
                // 反之
                String[] lines = linestr.split("\n");
                for (String line : lines) {
                    String[] kv = line.split("=");
                    preparedStatement.setString(0b01, kv[0]);
                    preparedStatement.setString(0b10, kv[1]);
                    preparedStatement.addBatch();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将某一个范围的数据输出到数据库表中
     *
     * @param b   需要被区间切分输出的数据byte数组
     * @param off 左闭区间
     * @param len 右闭区间
     * @throws IOException 数据输入异常
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        byte[] data = new byte[len - off + 1];
        int DN = 0;
        for (int n = off; n <= len; n++) {
            data[DN] = b[n];
            DN++;
        }
        write(data);
    }

    /**
     * 必须要调用 将所有的缓存命令一起执行
     *
     * @throws IOException 执行SQL异常
     */
    @Override
    public void flush() throws IOException {
        try {
            preparedStatement.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭数据操作对象
     *
     * @throws IOException 关闭失败异常
     */
    @Override
    public void close() throws IOException {
        try {
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
