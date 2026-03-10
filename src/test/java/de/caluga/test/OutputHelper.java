package de.caluga.test;

import com.colofabrix.java.figlet4s.Figlet4s;
import com.colofabrix.java.figlet4s.options.HorizontalLayout;
import com.colofabrix.java.figlet4s.options.Justification;
import com.colofabrix.java.figlet4s.options.PrintDirection;
import com.colofabrix.java.figlet4s.options.RenderOptions;
import org.slf4j.Logger;

public class OutputHelper {

    public static void figletOutput(Logger log, String message) {
        var font = Figlet4s.loadFontInternal("starwars");
        RenderOptions options = new RenderOptions(
                font,
                100,
                HorizontalLayout.HORIZONTAL_FITTING,
                PrintDirection.LEFT_TO_RIGHT,
                Justification.FONT_DEFAULT);
        for (var l : Figlet4s.renderString(message, options).asList()) {
            log.info(l);
        }
    }
}

