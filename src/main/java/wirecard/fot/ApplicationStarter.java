package wirecard.fot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplicationStarter {

    private static Logger logger = LoggerFactory.getLogger("ApplicationStarter");

    public static void main(String[] args) {
        logger.info("Starting the whole thing ...");
        OffsetSetter.setOffsets();
        TopologyCopying.startCopying();
    }

}
