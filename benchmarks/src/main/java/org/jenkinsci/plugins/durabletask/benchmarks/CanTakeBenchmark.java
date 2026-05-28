/*
 * The MIT License
 *
 * Copyright (c) 2013-2014, CloudBees, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package org.jenkinsci.plugins.durabletask.benchmarks;

import hudson.model.Label;
import hudson.model.Node;
import hudson.model.Queue;
import hudson.model.ResourceList;
import hudson.model.queue.AbstractQueueTask;
import hudson.model.queue.SubTask;
import org.jenkinsci.plugins.durabletask.executors.ContinuedTask;
import org.mockito.MockedStatic;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Benchmarks {@link ContinuedTask.Scheduler#canTake} under a large queue.
 *
 * <p>Two scenarios are measured:
 * <ul>
 *   <li><b>allRegular</b> – queue contains only non-continued items; every iteration
 *       walks the full list and emits a {@code LOGGER.finer} call per item. This is
 *       the hot path that degrades with 1000+ FreeStyleProjects.</li>
 *   <li><b>oneContinued</b> – one continued item sits at the front of the queue;
 *       {@code canTake} returns a {@link hudson.model.queue.CauseOfBlockage} after
 *       the first match, exercising the early-exit path.</li>
 * </ul>
 *
 * <p>Run via Maven:
 * <pre>
 *   mvn -pl benchmarks verify
 * </pre>
 * Or directly after {@code mvn -pl benchmarks package}:
 * <pre>
 *   java -jar benchmarks/target/benchmarks.jar [regexp] [JMH options]
 * </pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@State(Scope.Thread)
public class CanTakeBenchmark {

    // -------------------------------------------------------------------------
    // Parameters
    // -------------------------------------------------------------------------

    /** Number of items in the simulated queue. */
    @Param({"100", "500", "1000", "2000"})
    public int queueSize;

    /**
     * JUL log level applied to the root logger for the duration of the trial.
     * <ul>
     *   <li>{@code OFF} – measures pure scheduler logic with no logging overhead.</li>
     *   <li>{@code FINER} – reproduces the production scenario: every non-continued
     *       item in the queue causes a {@code LOGGER.finer} call whose supplier lambda
     *       is evaluated even when a log4j2-JUL bridge is not listening below INFO.</li>
     * </ul>
     */
    @Param({"OFF", "FINER"})
    public String logLevel;

    // -------------------------------------------------------------------------
    // State
    // -------------------------------------------------------------------------

    private ContinuedTask.Scheduler scheduler;
    private Node node;

    /** The item being evaluated by canTake (a plain FreeStyleProject stand-in). */
    private Queue.BuildableItem incomingItem;

    private List<Queue.BuildableItem> allRegularQueue;
    private List<Queue.BuildableItem> oneContinuedQueue;

    private MockedStatic<Queue> queueMock;

    // -------------------------------------------------------------------------
    // Setup / teardown
    // -------------------------------------------------------------------------

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        Logger.getLogger("").setLevel(java.util.logging.Level.parse(logLevel));

        scheduler = new ContinuedTask.Scheduler();
        node = mock(Node.class);

        // The item whose scheduling is being decided – a plain (non-continued) task.
        PlainTask incomingTask = new PlainTask("incoming");
        incomingItem = buildableItem(incomingTask);

        // Build the two queue snapshots once per trial.
        allRegularQueue = new ArrayList<>(queueSize);
        for (int i = 0; i < queueSize; i++) {
            allRegularQueue.add(buildableItem(new PlainTask("regular-" + i)));
        }

        oneContinuedQueue = new ArrayList<>(queueSize);
        // First item is continued – canTake should block immediately.
        oneContinuedQueue.add(buildableItem(new ContinuedPlainTask("continued-0")));
        for (int i = 1; i < queueSize; i++) {
            oneContinuedQueue.add(buildableItem(new PlainTask("regular-" + i)));
        }

        // Mock Queue.getInstance() so canTake never touches a real Jenkins instance.
        Queue queueInstance = mock(Queue.class);
        queueMock = mockStatic(Queue.class);
        queueMock.when(Queue::getInstance).thenReturn(queueInstance);
        // Default to the all-regular snapshot; individual benchmarks override this.
        when(queueInstance.getBuildableItems()).thenReturn(allRegularQueue);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        queueMock.close();
    }

    // -------------------------------------------------------------------------
    // Benchmarks
    // -------------------------------------------------------------------------

    /**
     * All queue items are plain (non-continued). {@code canTake} iterates every
     * item and calls {@code LOGGER.finer} for each one before returning {@code null}.
     * This is the scenario that degrades with large queues when the log4j2-JUL
     * bridge is active, even at INFO level.
     */
    @Benchmark
    public Object allRegular() {
        setQueueSnapshot(allRegularQueue);
        return scheduler.canTake(node, incomingItem);
    }

    /**
     * One continued item is at the front of the queue. {@code canTake} finds it
     * immediately and returns a {@link hudson.model.queue.CauseOfBlockage} without
     * scanning the rest of the list.
     */
    @Benchmark
    public Object oneContinued() {
        setQueueSnapshot(oneContinuedQueue);
        return scheduler.canTake(node, incomingItem);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void setQueueSnapshot(List<Queue.BuildableItem> snapshot) {
        Queue q = Queue.getInstance();
        when(q.getBuildableItems()).thenReturn(snapshot);
    }

    /**
     * Constructs a {@link Queue.BuildableItem} from a task without a running Jenkins.
     * {@code WaitingItem} is the only public constructor path into {@code BuildableItem}.
     */
    private static Queue.BuildableItem buildableItem(Queue.Task task) {
        Calendar timestamp = Calendar.getInstance();
        Queue.WaitingItem waiting = new Queue.WaitingItem(timestamp, task, Collections.emptyList());
        return new Queue.BuildableItem(waiting);
    }

    // -------------------------------------------------------------------------
    // Minimal Queue.Task implementations
    // -------------------------------------------------------------------------

    private static class PlainTask extends AbstractQueueTask {
        private final String name;

        PlainTask(String name) {
            this.name = name;
        }

        @Override public String getName() { return name; }
        @Override public String getFullDisplayName() { return name; }
        @Override public String getDisplayName() { return name; }
        @Override public String getUrl() { return "mock/" + name; }
        @Override public Label getAssignedLabel() { return null; }
        @Override public Node getLastBuiltOn() { return null; }
        @Override public long getEstimatedDuration() { return -1; }
        @Override public ResourceList getResourceList() { return new ResourceList(); }
        @Override public boolean isBuildBlocked() { return false; }
        @Override public String getWhyBlocked() { return null; }
        @Override public void checkAbortPermission() {}
        @Override public boolean hasAbortPermission() { return true; }
        @Override public String toString() { return name; }

        @Override
        public Queue.Executable createExecutable() throws IOException {
            return new Queue.Executable() {
                @Override public SubTask getParent() { return PlainTask.this; }
                @Override public long getEstimatedDuration() { return -1; }
                @Override public void run() {}
            };
        }
    }

    /** A {@link ContinuedTask} that always reports {@code isContinued() == true}. */
    private static final class ContinuedPlainTask extends PlainTask implements ContinuedTask {
        ContinuedPlainTask(String name) { super(name); }

        @Override public boolean isContinued() { return true; }
    }

    // -------------------------------------------------------------------------
    // Main – allows running directly without Maven
    // -------------------------------------------------------------------------

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CanTakeBenchmark.class.getSimpleName())
                .forks(1)
                .warmupIterations(3)
                .measurementIterations(5)
                .build();
        new Runner(opt).run();
    }
}
