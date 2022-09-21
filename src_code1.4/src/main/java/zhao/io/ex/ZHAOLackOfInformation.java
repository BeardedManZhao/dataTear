package zhao.io.ex;

/**
 * API调用异常
 */
public class ZHAOLackOfInformation extends NullPointerException {

    public ZHAOLackOfInformation(String s) {
        super(s);
    }

    @Override
    public String getMessage() {
        return super.getMessage() + "您的API调用设置不全哦！！";
    }
}
