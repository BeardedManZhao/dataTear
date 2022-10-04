package zhao.io.dataTear.dataContainer;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

/**
 * DataTear数据容器，是内存中存储数据的地方
 * <p>
 * NameManager的拓展类，支持与数据碎片进行对接
 * <p>
 * DataTear data container, which is an extension class of NameManager where data is stored in memory, supports docking with data fragments
 *
 * @param <T> 表中的每一个字段类型 Each field type in the table
 */
public class RWTable<T> extends NameManager<T> implements RWData<T[]> {
    private final HashMap<Integer, DataFragmentation> FragmentationMap = new HashMap<>();
    private int primaryKeyNum = 0;
    private long TableCount = 0L;

    public RWTable() {
        // 同步数据块信息
        for (int n = 0; n < getFragmentationNum(); n++) {
            FragmentationMap.put(n, new DataFragmentation(n));
            getDataFragmentation_Manager().computeIfAbsent(n, k -> new HashSet<>());
        }
    }

    /**
     * 该方法通常用来通过数据碎片编号，获取数据碎片类
     * <p>
     * This method is usually used to obtain the data fragment class through the data fragment number
     *
     * @return 数据碎片编号与对应的数据碎片类
     * <p>
     * Data fragment number and corresponding data fragment clas
     */
    public HashMap<Integer, DataFragmentation> getFragmentationMap() {
        return FragmentationMap;
    }

    /**
     * DataTear碎片的数量，用于取余轮询添加数据到数据碎片类
     * <p>
     * The number of Data Tear shards, used to add data to the data shard class by taking the remainder of the polling
     *
     * @param FragmentationNum 数据碎片的数量 The number of data fragments
     */
    @Override
    public void setFragmentationNum(int FragmentationNum) {
        super.FragmentationNum = FragmentationNum;
        for (int n = 0; n < getFragmentationNum(); n++) {
            FragmentationMap.put(n, new DataFragmentation(n));
            getDataFragmentation_Manager().computeIfAbsent(n, k -> new HashSet<>());
        }
    }

    /**
     * @return 构建索引时使用的主键索引编号
     * <p>
     * The primary key index number to use when building the index
     */
    public int getPrimaryKeyNum() {
        return primaryKeyNum;
    }

    /**
     * @param primaryKeyNum 设置当前数据表中的主键字段索引
     *                      <p>
     *                      Set the primary key field index in the current data table
     * @return 链式 chain
     */
    public RWTable<T> setPrimaryKeyNum(int primaryKeyNum) {
        this.primaryKeyNum = primaryKeyNum;
        return this;
    }

    public Long Count() {
        return TableCount;
    }

    /**
     * 添加一行数据到NameManager中
     * <p>
     * Add a row of data to Name Manager
     *
     * @param data 需要添加的一行数据 A row of data to be added
     */
    @Override
    public void putData(T[] data) {
        // 构建这一行数据的行编号
        int lineNum = (int) TableCount;
        // 取余轮询计算碎片编号
        int FragmentationNum = lineNum % getFragmentationNum();
        // 将主键数据添加到nameManager对应编号的注解列表中
        getDataFragmentation_Manager().get(FragmentationNum).add(data[primaryKeyNum]);
        // 将主键数据对应的行数据，添加到对应碎片编号的类中进行存储
        this.FragmentationMap.get(FragmentationNum).addRowData((String[]) data);
        TableCount += 0b1;
    }

    /**
     * 批量添加数据
     * 这里会对主键进行索引构建
     * 然后将数据使用轮询的方式，标记到对应的数据块区域中
     * 后面会严格按照数据块区域的划分写数据到数据块
     * <p>
     * Add data in batches. The primary key will be indexed here, and then the data will be marked in the corresponding data block area by polling. Later, the data will be written to the data block strictly according to the division of the data block area.
     *
     * @param lines 需要添加的所有数据行们  All data rows that need to be added
     */
    public void putAllData(Collection<T[]> lines) {
        // 获取每一行数据
        for (T[] data : lines) {
            putData(data);
        }
    }

    @Override
    @Deprecated
    public T[] getData() {
        return null;
    }

    public String superString() {
        return super.toString();
    }

    @Override
    public String toString() {
        return "\nZHAOZHAOZHAOZHAOZHAOZHAO\n";
    }
}
