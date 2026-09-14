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
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Implementation of ModelingEngine that uses ArrayBlockingQueue objects for
 * synchronization between threads, with an explicit drain-turn lock so two
 * workers cannot mutate the waiter queue at once.
 *
 * Small-patch hardening of the existing worker design:
 * - ReentrantLock owns the drain loop (one turn)
 * - waiters queue is synchronized
 * - resume put failure is an Error, not a silent retire
 * - daemon activity workers and shutdownNow() so a dropped turn cannot freeze the JVM
 */
public class FunctionalWaitModelingEngine extends ModelingEngine implements WaitProvider {

    private static final Logger LOGGER = Logger.getLogger(FunctionalWaitModelingEngine.class.getName());

    private enum Result {
        Waiting,
        Error,
        Done
    }

    private final PriorityQueue<Waiter> waiters = new PriorityQueue<>();
    private final Object waitersLock = new Object();
    private final ReentrantLock turn = new ReentrantLock();
    private final BlockingQueue<Result> workerToMain = new ArrayBlockingQueue<>(8);
    private Exception thrownException = null;
    private boolean inmodel = false;

    FunctionalWaitModelingEngine()
    {
    }

    @Override
    protected void runModeling() {
        synchronized (waitersLock) {
            waiters.clear();
        }
        ActivityInstanceList activities = ActivityInstanceList.getActivityList();
        for (int i = 0; i < activities.length(); i++) {
            insertActivityIntoEngine(activities.get(i));
        }

        Time currentModelingTime;
        synchronized (waitersLock) {
            currentModelingTime = waiters.isEmpty() ? null : waiters.peek().getTime();
        }
        if (currentModelingTime == null) {
            currentModelingTime = new Time();
        }

        initTime(currentModelingTime);
        thrownException = null;

        final AtomicInteger n = new AtomicInteger();
        ExecutorService threadPool = Executors.newCachedThreadPool(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "blackbird-worker-" + n.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        });
        try {
            inmodel = true;
            threadPool.submit(this::runModelingThread);

            Result res = Result.Error;
            while (res != Result.Done) {
                res = workerToMain.take();
                if (res == Result.Error) {
                    throw new RuntimeException(thrownException);
                }
                if (res == Result.Waiting) {
                    threadPool.submit(this::runModelingThread);
                }
            }
        }
        catch (InterruptedException ex) {
            thrownException = ex;
        }
        finally {
            threadPool.shutdownNow();
            inmodel = false;
            if (turn.isHeldByCurrentThread()) {
                turn.unlock();
            }
        }
    }

    private void runModelingThread() {
        if (!turn.tryLock()) {
            LOGGER.fine("worker skipped drain; another thread already holds the modeling turn");
            return;
        }
        try {
            try {
                while (true) {
                    Waiter next;
                    synchronized (waitersLock) {
                        if (waiters.isEmpty()) {
                            break;
                        }
                        next = waiters.remove();
                    }
                    setTime(next.getTime());
                    Waiter future = next.execute();
                    if (next.resumedAThread()) {
                        return;
                    }
                    if (future != null) {
                        insertWaiter(future);
                    }
                }
                workerToMain.put(Result.Done);
            }
            catch (RuntimeException ex) {
                thrownException = ex;
                workerToMain.put(Result.Error);
            }
        }
        catch (InterruptedException ex) {
            // interrupted during shutdown
        }
        finally {
            if (turn.isHeldByCurrentThread()) {
                turn.unlock();
            }
        }
    }

    @Override
    void insertWaiter(Waiter toInsert) {
        if (inmodel && toInsert.getTime().lessThan(getCurrentTime())) {
            throw new RuntimeException(String.format(
                    "Current time is [%s]. Cannot schedule an event at [%s] because it is in the past.",
                    getCurrentTime().toString(), toInsert.getTime().toString()));
        }
        synchronized (waitersLock) {
            waiters.add(toInsert);
        }
    }

    @Override
    public void insertActivityIntoEngine(Activity activity) {
        activity.setThread(this);
        insertWaiter(new Waiter(activity.getStart(), 1, () -> {
            ActivityTypeList.getActivityList().propertyChangeForActivityType(activity.getType(), activity);
            try {
                return activity.modelFunc();
            }
            catch (InterruptedException ex) {
                return null;
            }
        }));
    }

    private static void deliver(BlockingQueue<Object> queue, Object token) {
        try {
            queue.put(token);
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while handing off modeling turn", ex);
        }
    }

    @Override
    public void waitUntil(Time t) {
        try {
            final BlockingQueue<Object> queue = new ArrayBlockingQueue<Object>(1);
            Waiter waiter = new Waiter(t, 0, () -> {
                deliver(queue, FunctionalWaitModelingEngine.this);
                return null;
            });
            waiter.willResumeThread(true);
            insertWaiter(waiter);
            if (turn.isHeldByCurrentThread()) {
                turn.unlock();
            }
            workerToMain.put(Result.Waiting);
            queue.take();
            turn.lock();
        }
        catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Map waitForSignal(String signalName) throws InterruptedException {
        final BlockingQueue<Object> queue = new ArrayBlockingQueue<Object>(1);
        Signal.getSignal(signalName).addSignalHandler(true, (m) -> {
            deliver(queue, m == null ? new HashMap() : m);
            return null;
        });
        if (turn.isHeldByCurrentThread()) {
            turn.unlock();
        }
        workerToMain.put(Result.Waiting);
        Object payload = queue.take();
        turn.lock();
        @SuppressWarnings("unchecked")
        Map result = (Map) payload;
        return result;
    }

    @Override
    public Waiter waitUntil(Time t, Supplier<Waiter> func) {
        return new Waiter(t, 0, func);
    }

    @Override
    public Waiter waitForSignal(String signalName, Function<Map, Waiter> func) {
        Signal.getSignal(signalName).addSignalHandler(false, func);
        return null;
    }
}
