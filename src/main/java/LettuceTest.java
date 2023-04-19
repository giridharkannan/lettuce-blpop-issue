import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.concurrent.locks.LockSupport;

public class LettuceTest {

    private static final String QUEUE_KEY = "test_queue";



    public static void main(String[] args) {
        LettuceTest test = new LettuceTest();
        Thread t = new Thread(test::produce);
        t.start();
        test.consume(t);
    }

    private void consume(Thread producer) {
        RedisClient c = RedisClient.create(RedisURI.Builder.redis("127.0.0.1", 6379).build());

        RedisCommands<String, String> cmd = c.connect().sync();
        while (true) {
            try {
                KeyValue<String, String> resp = cmd.brpop(0, QUEUE_KEY);
                if (resp == null) continue;
                System.out.println(">>" + resp.getValue());
            } catch (RedisCommandTimeoutException e) {
                e.printStackTrace();
                LockSupport.unpark(producer);
            }
        }
    }

    private void produce() {
        RedisClient c = RedisClient.create(RedisURI.Builder.redis("127.0.0.1", 6379).build());
        RedisCommands<String, String> cmd = c.connect().sync();

        LockSupport.park(); // wait for connection timeout

        System.out.println("Going to push message");
        sleep(1);

        cmd.lpush(QUEUE_KEY, "1st will not come"); //Even though lettuce receives this message, it will not be sent

        cmd.lpush(QUEUE_KEY, "2nd will come");
        cmd.lpush(QUEUE_KEY, "3nd will come");
    }

    private void sleep(long sec) {
        try {
            Thread.sleep(sec * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
