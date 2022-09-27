package zhao.io.dataTear.dataContainer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * 数据碎片类，其中包含一个数据碎片所需要的数据与数据索引
 * <p>
 * Data fragmentation class, which contains the data and data indexes required by a data fragmentation
 * <p>
 * 创建时间：2022-07-25
 * <p>
 * Creation time: 2022-07-25
 *
 * @author 赵凌宇
 * @version 1.0
 */
public class DataFragmentation implements RWData<String> {
    /**
     * 数据碎片的编号  The number of the data fragment
     */
    private final int FragmentationNum;

    /**
     * 数据碎片中包含的数据行们  The data rows contained in the data fragment
     */
    private final ArrayList<String[]> lines = new ArrayList<>();

    /**
     * 数据输出分隔符  data output delimiter
     */
    private String outSplitChar = ",";

    /**
     * 构造一个数据碎片类  Construct a data fragment class
     *
     * @param fragmentationNum 碎片编号 每一个被管理的数据碎片都有一个独一无二的ID 需要通过该参数构造出来一个数据碎片
     *                         <p>
     *                         Fragment number Each managed data fragment has a unique ID. It is necessary to construct a data fragment through this parameter.
     */
    public DataFragmentation(int fragmentationNum) {
        FragmentationNum = fragmentationNum;
    }

    /**
     * 设置数据输出分隔符  Set data output delimiter
     *
     * @param outSplitChar 分割字符串 默认是逗号  split string default is comma
     */
    public void setOutSplitChar(String outSplitChar) {
        this.outSplitChar = outSplitChar;
    }

    /**
     * 提取该数据碎片的编号  Extract the number of the data fragment
     *
     * @return 数据碎片的编号int数值  The number of the data fragment int value
     */
    public int getFragmentationNum() {
        return FragmentationNum;
    }

    /**
     * 向碎片中添加一行数据，参数每一个单元格中，需要使用逗号做数据分隔符
     * <p>
     * Add a row of data to the fragment. In each cell of the parameter, you need to use a comma as the data separator
     *
     * @param data 需要被添加的一行数据  A row of data to be added
     */
    @Override
    public void putData(String data) {
        lines.add(data.split(","));
    }

    /**
     * 快速的添加很多数据，这里用来添加一行数据，效果与 putData效果相同，只是参数不同
     * <p>
     * Quickly add a lot of data, here is used to add a row of data, the effect is the same as putData, but the parameters are different
     *
     * @param line 需要被添加的一行数据，一个元素是一个单元格  A row of data to be added, an element is a cell
     */
    public void addRowData(String[] line) {
        this.lines.add(line);
    }

    /**
     * 快速的添加很多数据，添加一张表的所有数据行的集合
     * <p>
     * Quickly add a lot of data, add a collection of all data rows in a table
     *
     * @param lines 需要被添加的所有数据行容器，每一个元素就是一行数据，每一个元素是一个数组，数组包含的每一个数值就是一个单元格
     *              <p>
     *              All data row containers that need to be added, each element is a row of data, each element is an array, and each value contained in the array is a cell
     */
    public void addAllData(Collection<String[]> lines) {
        this.lines.addAll(lines);
    }

    /**
     * 将本碎片的数据按照规格返回出去，便于外界输出
     * <p>
     * Return the data of this fragment according to the specifications, which is convenient for external output
     *
     * @return 需要被文件存储格式操作的数据  Data that needs to be manipulated by the file storage format
     */
    @Override
    public String getData() {
        StringBuilder stringBuilder = new StringBuilder();
        this.lines.forEach(line -> stringBuilder.append(Arrays.stream(line).reduce((x, y) -> x + outSplitChar + y).orElse("---NULL---")).append("\n"));
        return stringBuilder.toString();
    }

    /**
     * 查找包含该单元格数据的所有行数据，用于进行数据碎片内部的数据查找
     * <p>
     * Find all row data containing the cell data, used for data lookup inside the data fragment
     *
     * @param cellValue 单元格数据 cell data
     * @return 包含很多行的列表 其中每一个元素是一行，一行数据由很多单元格组成数组
     * <p>
     * A list with many rows where each element is a row, and a row of data consists of an array of many cells
     */
    public ArrayList<String[]> getValueByCell(String cellValue) {
        return this.lines.stream()
                .filter(strings -> Arrays.asList(strings).contains(cellValue))
                .collect(Collectors.toCollection(ArrayList::new));
    }
}
