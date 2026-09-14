package gov.nasa.jpl.engine;

import gov.nasa.jpl.activity.Activity;
import gov.nasa.jpl.common.BaseTest;
import gov.nasa.jpl.resource.IntegerResource;
import gov.nasa.jpl.time.Duration;
import gov.nasa.jpl.time.Time;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Regression tests for engine thread handoff around blocking waitUntil / waitForSignal.
 */
public class EngineConcurrencyTest extends BaseTest {

    public static class SteppingActivity extends Activity {
        private final IntegerResource res;
        private final Duration step;
        private final int hops;

        public SteppingActivity(Time t, IntegerResource res, Duration step, int hops) {
            super(t);
            this.res = res;
            this.step = step;
            this.hops = hops;
        }

        @Override
        public void model() {
            for (int i = 0; i < hops; i++) {
                res.add(1);
                waitFor(step);
            }
            res.add(1);
        }
    }

    public static class WaitUntilNowActivity extends Activity {
        private final IntegerResource res;

        public WaitUntilNowActivity(Time t, IntegerResource res) {
            super(t);
            this.res = res;
        }

        @Override
        public void model() {
            res.add(1);
            waitUntil(now());
            res.add(10);
        }
    }

    public static class BlockingSignalWaiter extends Activity {
        public BlockingSignalWaiter(Time t) {
            super(t);
        }

        @Override
        public void model() throws InterruptedException {
            Map payload = waitForSignal("engine-concurrency");
            if (payload != null && payload.containsKey("n")) {
                payload.get("n");
            }
        }
    }

    public static class BlockingSignalSender extends Activity {
        public BlockingSignalSender(Time t) {
            super(t);
        }

        @Override
        public void model() {
            Map<String, Object> payload = new HashMap<String, Object>();
            payload.put("n", 1);
            Signal.send("engine-concurrency", payload);
        }
    }

    @Test(timeout = 15000)
    public void manySteppingActivitiesComplete() {
        IntegerResource res = new IntegerResource();
        Time t0 = Time.getDefaultReferenceTime();
        for (int i = 0; i < 8; i++) {
            new SteppingActivity(t0.add(Duration.SECOND_DURATION.multiply(i)), res, Duration.SECOND_DURATION, 4);
        }
        ModelingEngine.getEngine().model();
        assertEquals(40, (int) res.currentval());
    }

    @Test(timeout = 15000)
    public void waitUntilCurrentTimeDoesNotDeadlock() {
        IntegerResource res = new IntegerResource();
        Time t0 = Time.getDefaultReferenceTime();
        new WaitUntilNowActivity(t0, res);
        new WaitUntilNowActivity(t0, res);
        new WaitUntilNowActivity(t0.add(Duration.SECOND_DURATION), res);
        ModelingEngine.getEngine().model();
        assertEquals(33, (int) res.currentval());
    }

    @Test(timeout = 15000)
    public void blockingWaitForSignalCompletes() {
        Time t0 = Time.getDefaultReferenceTime();
        new BlockingSignalWaiter(t0);
        new BlockingSignalSender(t0.add(Duration.SECOND_DURATION));
        ModelingEngine.getEngine().model();
    }
}
