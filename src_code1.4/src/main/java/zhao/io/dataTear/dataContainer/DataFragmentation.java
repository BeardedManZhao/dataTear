package zhao.io.dataTear.dataContainer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * 数据碎片类，其中包含一个数据碎片所需要的数据与写时索引
 * <p>
 * 创建时间：2022-07-25
 *
 * @author 赵凌宇
 * @version 1.0
 */
public class DataFragmentation implements RWData<String> {
    /**
     * 数据碎片的编号
     */
    private final int FragmentationNum;

    /**
     * 数据碎片中包含的数据行们
     */
    private final ArrayList<String[]> lines = new ArrayList<>();
    /**
     * 数据输出分隔符
     */
    private String outSplitChar = ",";

    /**
     * 构造一个数据碎片类
     *
     * @param fragmentationNum 碎片编号
     */
    public DataFragmentation(int fragmentationNum) {
        FragmentationNum = fragmentationNum;
    }

    /**
     * 设置数据输出分隔符
     *
     * @param outSplitChar 分割字符串 默认是逗号
     */
    public void setOutSplitChar(String outSplitChar) {
        this.outSplitChar = outSplitChar;
    }

    /**
     * 提取该数据碎片的编号
     *
     * @return 数据碎片的编号int数值
     */
    public int getFragmentationNum() {
        return FragmentationNum;
    }

    /**
     * 向碎片中添加一行数据，参数每一个单元格中，需要使用逗号做数据分隔符
     *
     * @param data 添加一行数据
     */
    @Override
    public void putData(String data) {
        lines.add(data.split(","));
    }

    /**
     * 快速的添加很多数据，这里用来添加一行数据
     *
     * @param line 需要被添加的一行数据
     */
    public void addRowData(String[] line) {
        this.lines.add(line);
    }

    /**
     * 快速的添加很多数据，添加一张表的所有数据行的集合
     *
     * @param lines 需要被添加的所有数据行
     */
    public void addAllData(Collection<String[]> lines) {
        this.lines.addAll(lines);
    }

    /**
     * 将本碎片的数据按照规格返回出去，便于外界输出
     *
     * @return 需要被文件存储格式操作的数据
     */
    @Override
    public String getData() {
        StringBuilder stringBuilder = new StringBuilder();
        this.lines.forEach(line -> stringBuilder.append(Arrays.stream(line).reduce((x, y) -> x + outSplitChar + y).orElse("---NULL---")).append("\n"));
        return stringBuilder.toString();
    }

    /**
     * 查找包含单元格的所有行数据
     *
     * @param cellValue 单元格数据
     * @return 包含很多行的列表 其中每一个元素是一行，一行数据由很多单元格组成数组
     */
    public ArrayList<String[]> getValue(String cellValue) {
        return this.lines.stream()
                .filter(strings -> Arrays.asList(strings).contains(cellValue))
                .collect(Collectors.toCollection(ArrayList::new));
    }
}
