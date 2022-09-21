package zhao.io.dataTear.dataContainer;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

/**
 * DataTear数据容器，是内存中存储数据的地方
 * <p>
 * NameManager的拓展类，支持与数据碎片进行对接
 *
 * @param <T> 表中的每一个字段类型
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
     *
     * @return 数据碎片编号与对应的数据碎片类
     */
    public HashMap<Integer, DataFragmentation> getFragmentationMap() {
        return FragmentationMap;
    }

    /**
     * DataTear碎片的数量，用于取余轮询添加数据到数据碎片类
     *
     * @param FragmentationNum 数据碎片的数量
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
     */
    public int getPrimaryKeyNum() {
        return primaryKeyNum;
    }

    /**
     * @param primaryKeyNum 设置当前数据表中的主键字段索引
     * @return 链式
     */
    public RWTable<T> setPrimaryKeyNum(int primaryKeyNum) {
        this.primaryKeyNum = primaryKeyNum;
        return this;
    }

    public Long Count() {
        return TableCount;
    }

    /**
     * 一条条的添加数据
     *
     * @param data 需要添加的一行数据
     */
    @Override
    public void putData(T[] data) {
        // 提取这一行数据的主键
        T primaryKey = data[primaryKeyNum];
        // 构建这一行数据的行编号
        int lineNum = (int) TableCount;
        // 取余轮询计算碎片编号
        int FragmentationNum = lineNum % getFragmentationNum();
        // 将主键数据添加到nameManager对应编号的注解列表中
        getDataFragmentation_Manager().get(FragmentationNum).add(primaryKey);
        // 将主键数据对应的行数据，添加到对应碎片编号的类中进行存储
        this.FragmentationMap.get(FragmentationNum).addRowData((String[]) data);
        TableCount += 0b1;
    }

    /**
     * 批量添加数据
     * 这里会对主键进行索引构建
     * 然后将数据使用轮询的方式，标记到对应的数据块区域中
     * 后面会严格按照数据块区域的划分写数据到数据块
     *
     * @param lines 需要添加的所有数据行们
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
