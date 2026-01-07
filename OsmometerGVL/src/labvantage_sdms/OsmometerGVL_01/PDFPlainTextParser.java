package labvantage_sdms.OsmometerGVL_01;

import java.io.*;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

//PDF parser for Plain Text PDF output

public class PDFPlainTextParser {
	// For SDMS: parse directly from InputStream
    public static String parsePDF(InputStream inputStream) throws Exception {
        try (PDDocument document = PDDocument.load(inputStream)) {
            PDFTextStripper stripper = new PDFTextStripper();
            return stripper.getText(document);
        }
    }
}
