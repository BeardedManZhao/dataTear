package zhao.io.ex;

/**
 * 解析元数据错误异常
 */
public class AnalysisMetadataException extends NullPointerException {
    public AnalysisMetadataException() {
        super();
    }

    public AnalysisMetadataException(String message) {
        super(message);
    }

    @Override
    public String getMessage() {
        return super.getMessage() + "NameManager的元数据解析失败！";
    }

    @Override
    public String getLocalizedMessage() {
        return super.getLocalizedMessage() + "NameManager的元数据解析失败！";
    }
}
