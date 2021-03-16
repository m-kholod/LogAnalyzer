import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.hadoop.io.Text;

import java.util.StringTokenizer;

public class LogParser {

    private String ipAddress;
    private static final InetAddressValidator validator = InetAddressValidator.getInstance();

    public void parse(String string) {
        StringTokenizer st = new StringTokenizer(string, " ");
        ipAddress = st.nextToken();
    }

    public void process(Text string) {
        parse(string.toString());
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public boolean isValidIpAddress() {
        return validator.isValid(ipAddress);
    }
}
