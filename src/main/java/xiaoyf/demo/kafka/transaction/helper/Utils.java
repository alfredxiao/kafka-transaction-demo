package xiaoyf.demo.kafka.transaction.helper;

import lombok.experimental.UtilityClass;

import java.util.Random;

@UtilityClass
public class Utils {

    private Random random = new Random();

    public static void nap() {
        try {
            Thread.sleep(random.nextInt(100));
        } catch (Exception e) {
            // nothing to do
        }
    }
}
