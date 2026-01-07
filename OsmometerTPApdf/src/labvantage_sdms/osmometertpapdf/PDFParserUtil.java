package labvantage_sdms.osmometertpapdf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PDFParserUtil {

    public static String extractOsmolality(String rawText) {
        // Pattern: Osmolality <spaces> [number] <spaces> mOsm/kg
        Pattern pattern = Pattern.compile("Osmolality\\s+(\\d+(\\.\\d+)?)\\s+mOsm/kg");
        Matcher matcher = pattern.matcher(rawText);
        if (matcher.find()) {
            return matcher.group(1); // Extract the numeric value
        }
        return "";
    }
}
