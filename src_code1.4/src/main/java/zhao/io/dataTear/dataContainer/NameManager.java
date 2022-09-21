package zhao.io.dataTear.dataContainer;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

/**
 * DataTear元数据管理者
 *
 * @param <T> 表数中的主键数据类型
 */
public class NameManager<T> {
    protected final Long NM_ID = new Date().getTime();
    /**
     * 每一个数据块中的数据索引范围，用于定位需要的数据存在于哪个数据块
     * 第一个是块编号 List中存档的是这个块中的数据的主键。
     */
    private final HashMap<Integer, HashSet<T>> dataFragmentation_Manager = new HashMap<>();
    /**
     * 被这个NameManager管理的碎片有多少个
     */
    protected int FragmentationNum = 3;

    /**
     * 碎片编号与主键的map集合
     *
     * @return 数据碎片编号与主键 维护碎片编号与主键的关系
     */
    public HashMap<Integer, HashSet<T>> getDataFragmentation_Manager() {
        return dataFragmentation_Manager;
    }

    /**
     * 这里一般是nameManager容器被创建时候的毫秒值
     *
     * @return NameManager的编号
     */
    public Long getNM_ID() {
        return NM_ID;
    }

    /**
     * @return 数据碎片中的数据数量
     */
    public int getFragmentationNum() {
        return FragmentationNum;
    }

    /**
     * DataTear碎片的数量，用于取余轮询添加数据到数据碎片类
     *
     * @param FragmentationNum 数据碎片的数量
     */
    public void setFragmentationNum(int FragmentationNum) {
        this.FragmentationNum = FragmentationNum;
        // 同步数据块信息
        for (int n = 1; n <= FragmentationNum; n++) {
            dataFragmentation_Manager.computeIfAbsent(n, k -> new HashSet<>());
        }
    }

    /**
     * @param FragmentationNum 块编号
     * @param indexList        对应数据块中的索引列表
     */
    public void addLimit(int FragmentationNum, HashSet<T> indexList) {
        if (dataFragmentation_Manager.size() < FragmentationNum) {
            dataFragmentation_Manager.put(FragmentationNum, indexList);
        }
    }

    /**
     * 绘制由此NameManager管理的数据块的主键们
     *
     * @return 绘制好的索引范围
     */
    public String getAllLimit() {
        StringBuilder stringBuilder = new StringBuilder();
        int indexNumCount = dataFragmentation_Manager.size();
        // 迭代每一个数据块
        for (int num = 0; num < indexNumCount; num++) {
            int indexNum = 1; // 单数据块中的索引编号
            // 迭代一个数据块中的所有主键
            for (T t : dataFragmentation_Manager.get(num)) {
                    /*
                      数据块N的索引1 = 主键1
                      数据块N的索引1 = 主键2
                      数据块N的索引1 = 主键3
                     */
                stringBuilder.append("Fragmentation-").append(num)
                        .append(".DT@")
                        .append(indexNum)
                        .append(" = ")
                        .append(t.toString())
                        .append("\n");
                indexNum += 1;
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
