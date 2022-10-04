package zhao.io.dataTear.dataContainer;

/**
 * String数据操作读写容器的拓展类 Extended class for data manipulation read and write containers
 */
public class RWString implements RWData<String> {

    /**
     * container
     */
    StringBuilder stringBuilder = new StringBuilder();

    /**
     * @param data 被添加到容器中的数据
     *             <p>
     *             data to be added to the container
     */
    @Override
    public void putData(String data) {
        stringBuilder.append(data);
    }

    /**
     * @return 从容器中取出来数据
     * <p>
     * Extract data from container
     */
    @Override
    public String getData() {
        return stringBuilder.toString();
    }
}
