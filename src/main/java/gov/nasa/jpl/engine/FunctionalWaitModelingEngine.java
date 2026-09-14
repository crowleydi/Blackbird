package gov.nasa.jpl.engine;

import gov.nasa.jpl.activity.Activity;
import gov.nasa.jpl.activity.ActivityInstanceList;
import gov.nasa.jpl.activity.ActivityTypeList;
import gov.nasa.jpl.activity.WaitProvider;
import gov.nasa.jpl.time.Time;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Modeling engine that keeps a single scheduler thread which only drains the
 * event queue. Activity {@code model()}/{@code modelFunc()} always run on a
 * pooled activity thread. Blocking waits park that activity thread; resume
 * waiters unpark it and then wait until it parks again or finishes.
 *
 * The scheduler itself never retires after a resume and never runs adaptation
 * code on its own stack, so a missed handoff cannot destroy the only drain
 * loop. Main is the scheduler: there is no workerToMain Waiting protocol.
 */
public class FunctionalWaitModelingEngine extends ModelingEngine implements WaitProvider {

    private enum ActivityStatus { Parked, Finished, Error }

    private static final class ActivityGate {
        private final BlockingQueue<ActivityStatus> toScheduler = new ArrayBlockingQueue<ActivityStatus>(1);
        private final BlockingQueue<Object> park = new SynchronousQueue<Object>();
        private Waiter finishedWaiter;
        private Throwable error;

        void parked() {
            putStatus(ActivityStatus.Parked);
        }

        void finished(Waiter future) {
            finishedWaiter = future;
            putStatus(ActivityStatus.Finished);
        }

        void error(Throwable t) {
            error = t;
            putStatus(ActivityStatus.Error);
        }

        void unpark() {
            try {
                park.put(Boolean.TRUE);
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while unparking activity thread", ex);
            }
        }

        void awaitUnpark() throws InterruptedException {
            park.take();
        }

        Waiter await() {
            try {
                ActivityStatus status = toScheduler.take();
                if (status == ActivityStatus.Parked) {
                    return null;
                }
                if (status == ActivityStatus.Finished) {
                    return finishedWaiter;
                }
                if (error instanceof RuntimeException) {
                    throw (RuntimeException) error;
                }
                if (error instanceof Error) {
                    throw (Error) error;
                }
                throw new RuntimeException(error);
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
        }

        private void putStatus(ActivityStatus status) {
            try {
                toScheduler.put(status);
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
        }
    }

    private final PriorityQueue<Waiter> waiters = new PriorityQueue<>();
    private final ConcurrentHashMap<Thread, ActivityGate> gates = new ConcurrentHashMap<Thread, ActivityGate>();
    private ExecutorService activityThreads;
    private boolean inmodel = false;

    FunctionalWaitModelingEngine()
    {
    }

    @Override
    protected void runModeling() {
        waiters.clear();
        gates.clear();
        ActivityInstanceList activities = ActivityInstanceList.getActivityList();
        for (int i = 0; i < activities.length(); i++) {
            insertActivityIntoEngine(activities.get(i));
        }

        Time currentModelingTime = waiters.isEmpty() ? null : waiters.peek().getTime();
        if (currentModelingTime == null) {
            currentModelingTime = new Time();
        }

        initTime(currentModelingTime);

        final AtomicInteger activityThreadCount = new AtomicInteger();
        activityThreads = Executors.newCachedThreadPool(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "blackbird-activity-" + activityThreadCount.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        });
        try {
            inmodel = true;
            while (!waiters.isEmpty()) {
                Waiter next = waiters.remove();
                setTime(next.getTime());
                Waiter future = next.execute();
                if (future != null) {
                    insertWaiter(future);
                }
            }
        }
        finally {
            activityThreads.shutdownNow();
            activityThreads = null;
            inmodel = false;
            gates.clear();
        }
    }

    private Waiter runOnActivityThread(final Supplier<Waiter> work) {
        if (activityThreads == null) {
            throw new IllegalStateException("Activity thread pool is not running");
        }
        final ActivityGate gate = new ActivityGate();
        activityThreads.execute(new Runnable() {
            @Override
            public void run() {
                gates.put(Thread.currentThread(), gate);
                try {
                    gate.finished(work.get());
                }
                catch (Throwable t) {
                    gate.error(t);
                }
                finally {
                    gates.remove(Thread.currentThread());
                }
            }
        });
        return gate.await();
    }

    @Override
    void insertWaiter(Waiter toInsert) {
        if (inmodel && toInsert.getTime().lessThan(getCurrentTime())) {
            throw new RuntimeException(String.format(
                    "Current time is [%s]. Cannot schedule an event at [%s] because it is in the past.",
                    getCurrentTime().toString(), toInsert.getTime().toString()));
        }
        waiters.add(toInsert);
    }

    @Override
    public void insertActivityIntoEngine(Activity activity) {
        activity.setThread(this);
        insertWaiter(new Waiter(activity.getStart(), 1, () -> {
            ActivityTypeList.getActivityList().propertyChangeForActivityType(activity.getType(), activity);
            return runOnActivityThread(() -> {
                try {
                    return activity.modelFunc();
                }
                catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            });
        }));
    }

    @Override
    public void waitUntil(Time t) {
        final ActivityGate gate = gateForCurrentThread();
        Waiter waiter = new Waiter(t, 0, () -> {
            gate.unpark();
            return gate.await();
        });
        insertWaiter(waiter);
        gate.parked();
        try {
            gate.awaitUnpark();
        }
        catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Map waitForSignal(String signalName) throws InterruptedException {
        final ActivityGate gate = gateForCurrentThread();
        final BlockingQueue<Map> payload = new ArrayBlockingQueue<Map>(1);
        Signal.getSignal(signalName).addSignalHandler(false, (m) -> {
            try {
                payload.put(m == null ? new HashMap() : m);
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while delivering signal", ex);
            }
            gate.unpark();
            return gate.await();
        });
        gate.parked();
        gate.awaitUnpark();
        return payload.take();
    }

    @Override
    public Waiter waitUntil(Time t, Supplier<Waiter> func) {
        return new Waiter(t, 0, func);
    }

    @Override
    public Waiter waitForSignal(String signalName, Function<Map, Waiter> func) {
        Signal.getSignal(signalName).addSignalHandler(false, (m) ->
                runOnActivityThread(() -> func.apply(m)));
        return null;
    }

    private ActivityGate gateForCurrentThread() {
        ActivityGate gate = gates.get(Thread.currentThread());
        if (gate == null) {
            throw new RuntimeException(
                    "Blocking wait was called from a thread that is not running an activity. " +
                    "Use the functional waitUntil(Time, Supplier)/waitForSignal(String, Function) APIs " +
                    "from scheduler-side callbacks.");
        }
        return gate;
    }
}
