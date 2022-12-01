package zhao.io.dataTear.dataContainer;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

/**
 * <h3>中文</h3>
 * DataTear元数据管理者，在用户读取一个DataTear文件的时候会先读取这个文件，其中包含着有关所有数据碎片的索引信息，用于定位指定的数据碎片位置
 * <h3>English</h3>
 * Data Tear metadata manager, when a user reads a Data Tear file, the file will be read first, which contains index information about all data fragments, used to locate the specified data fragment location
 *
 * @param <T> 表数据中的主键数据类型  Primary key data type in table data
 */
public class NameManager<T> {
    protected final Long NM_ID = new Date().getTime();
    /**
     * 每一个数据块中的数据索引范围，用于定位需要的数据存在于哪个数据块
     * 第一个是块编号 List中存档的是这个块中的数据的主键。
     * <p>
     * The data index range in each data block is used to locate which data block the required data exists in. The first one is the block number. The data archived in the List is the primary key of the data in this block.
     */
    private final HashMap<Integer, HashSet<T>> dataFragmentation_Manager = new HashMap<>(0b11000);

    /**
     * 被这个NameManager管理的碎片总数，默认是三个，在输出的时候会根据这个数值生成对应的数据碎片分配，在数据读取的时候会根据这个数值生成对应的碎片数据加载线程的监控
     * <p>
     * The total number of fragments managed by this Name Manager is three by default. When outputting, the corresponding data fragment allocation will be generated according to this value. When data is read, the corresponding fragment data will be generated according to this value. Data loading thread monitoring
     */
    protected int FragmentationNum = 3;

    /**
     * 碎片编号与主键的map集合  A map collection of shard numbers and primary keys
     *
     * @return 数据碎片编号与主键 维护碎片编号与主键的关系
     * <p>
     * Data fragment number and primary key Maintain the relationship between fragment number and primary key
     */
    public HashMap<Integer, HashSet<T>> getDataFragmentation_Manager() {
        return dataFragmentation_Manager;
    }

    /**
     * 这里一般是nameManager容器被创建时候的毫秒值
     * <p>
     * This is usually the millisecond value when the name Manager container was created
     *
     * @return NameManager的编号  Number of NameManager
     */
    public Long getNM_ID() {
        return NM_ID;
    }

    /**
     * @return 数据碎片中的数据数量
     * <p>
     * The amount of data in the data fragment
     */
    public int getFragmentationNum() {
        return FragmentationNum;
    }

    /**
     * DataTear碎片的数量，用于取余轮询添加数据到数据碎片类
     * <p>
     * The number of DataTear shards used to add data to the data shard class by taking the remainder of the poll
     *
     * @param FragmentationNum 数据碎片的数量  The number of data fragments
     */
    public void setFragmentationNum(int FragmentationNum) {
        this.FragmentationNum = FragmentationNum;
        new Thread(() -> {
            // 同步数据块信息
            for (int n = 1; n <= FragmentationNum; n++) {
                dataFragmentation_Manager.computeIfAbsent(n, k -> new HashSet<>());
            }
        }).start();
    }

    /**
     * @param FragmentationNum 数据碎片的编号  The number of the data fragment
     * @param indexList        对应数据碎片中的索引列表  List of indexes in the corresponding data fragment
     */
    public void addLimit(int FragmentationNum, HashSet<T> indexList) {
        if (dataFragmentation_Manager.size() < FragmentationNum) {
            dataFragmentation_Manager.put(FragmentationNum, indexList);
        }
    }

    /**
     * 绘制由此NameManager管理的数据块的主键们
     * <p>
     * Plot the primary keys of the data blocks managed by this Name Manager
     *
     * @return 绘制好的索引范围  plotted index range
     */
    public String getAllLimit() {
        StringBuilder stringBuilder = new StringBuilder();
        int indexNumCount = dataFragmentation_Manager.size();
        // 迭代每一个数据碎片
        for (int num = 0; num < indexNumCount; num++) {
            int indexNum = 1; // 单数据块中的索引编号
            // 迭代一个数据块中的所有主键
            for (T t : dataFragmentation_Manager.get(num)) {
                stringBuilder.append("Fragmentation-").append(num)
                        .append(".DT@")
                        .append(indexNum++)
                        .append(" = ")
                        .append(t.toString())
                        .append("\n");
            }
        }
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return "zhao.NameManager.id = " + NM_ID + "\n" +
                "zhao.FragmentationNum = " + FragmentationNum + "\n" +
                getAllLimit() +
                "zhao.NameManager.Fragmentation.path = ";
    }
}
