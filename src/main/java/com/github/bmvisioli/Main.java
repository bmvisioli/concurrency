package com.github.bmvisioli;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveTask;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import jdk.incubator.concurrent.ScopedValue;
import jdk.incubator.concurrent.StructuredTaskScope;

public class Main {

  /**
   *
   * <p>Use Case: Find users with an id between 0 and 10k</p>
   */
  public static void main(String[] args) {
    printYellow("Pid : " + ProcessHandle.current().pid());

    StopWatch.start();

    // Replace with one of the implementations
    sequential();

    StopWatch.stopAndPrint();
  }

  // Implementation

  /**
   *
   * <li>Blocks the main thread;</li>
   * <li>Executes commands sequentially (1 by 1);</li>
   * <li>Execution time is the sum of each process execution time (in this case ~ 3h);</li>
   */
  public static void sequential() {
    var results = run(_10k, index -> UserRepository.findUser(index));

    // Do some other work...
    mainThreadWork();

    printGreen(results);
  }

  /**
   *
   * <li>Platform Threads are 1-to-1 to OS Threads;</li>
   * <li>OS Threads are expensive (2MB stacktrace);</li>
   * <li>Context-switching is expensive;</li>
   * <li>Low-level API;</li>
   */
  public static void threads() {
    var results = new ArrayList<User>();

    var threads = run(_10k, index -> {
      var thread = new Thread(() -> UserRepository.findUser(index).ifPresent(results::add));
      thread.start();
      return thread;
    });

    // Do some other work...
    mainThreadWork();

    threads.forEach(Thread::join);

    printGreen(results);
  }

  /**
   *
   * <li>Executors manage the creation, scheduling and pooling of threads;</li>
   * <li>Most frequently uses a thread pool;</li>
   * <li>Each thread is still 1-1 with OS Threads;</li>
   * <li>Returns a Future with a wrapped typed (higher-level API).</li>
   * <img src="https://www.baeldung.com/wp-content/uploads/2016/08/2016-08-10_10-16-52-1024x572.png"/>
   */
  public static void executors() {
    try (var executor = Executors.newFixedThreadPool(1000)) {

      var futures = run(_10k, index ->
          executor.submit(() -> UserRepository.findUser(index))
      );

      // Do some other work...
      mainThreadWork();

      var results = futures
          .map(Future::get)
          .flatMap(Optional::stream)
          .collect(Collectors.toList());

      printGreen(results);
    }
  }

  /**
   *
   * <li>Used to form a pipeline;</li>
   * <li>Wraps around old Futures created by executors;</li>
   * <li>By default uses ForkJoinPool.commonPool...</li>
   * <li>... unless another Executor is specified;</li>
   * <li>Causes function colouring;</li>
   */
  public static void completableFutures() {
    try (var executor = Executors.newFixedThreadPool(1000)) {

      var completableFutures = run(_10k, index ->
          CompletableFuture.supplyAsync(() -> UserRepository.findUser(index), executor)
      );

      // Do some other work
      mainThreadWork();

      CompletableFuture.allOf(completableFutures.toArray(CompletableFuture.emptyArray()))
          .thenApply(__ ->
              completableFutures.stream()
                  .map(CompletableFuture::get)
                  .flatMap(Optional::stream)
                  .collect(Collectors.toList())
          )
          .thenAccept(Main::printGreen)
          .join();
    }
  }

  /**
   *
   * <li>Virtual threads are cheap because...;</li>
   * <li>... their stack can have a dynamic size ...;</li>
   * <li>... since it is stored in the heap space;</li>
   * <li>Seamless parallelism;</li>
   * <li>Example: Thread-per-request is practical again;</li>
   * <li>Loom goal: Integrate well wherever Threads and Executors are used;</li>
   * <li>Great for blocking workloads, makes no difference for CPU-bound;</li>
   * <img src="https://i0.wp.com/theboreddev.com/wp-content/uploads/2022/11/scheduling.png?resize=780%2C358&ssl=1"/>
   */
  public static void virtualThreads() {
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = run(_10k, index ->
          executor.submit(() -> UserRepository.findUser(index))
      );

      // Do some other work
      mainThreadWork();

      var results = futures
          .map(Future::get)
          .flatMap(Optional::stream)
          .collect(Collectors.toList());

      printGreen(results);
    }
  }

  /**
   *
   * Find two users and compare their departments
   */
  public static void completableFutures2() {
    CompletableFuture.supplyAsync(() -> UserRepository.findUser(0))
        .thenCompose(user0Opt ->
            CompletableFuture.supplyAsync(() -> UserRepository.findUser(666))
                .thenApply(user666Opt -> user0Opt.map(User::department).equals(user666Opt.map(User::department)))
        )
        .whenComplete((result, exception) -> {
          if (exception instanceof IllegalStateException) {
            printRed("Got an IllegalStateException");
          } else if (exception != null) {
            exception.printStackTrace();
          } else {
            printGreen("CompletableFuture result: " + result);
          }
        })
        .join();
  }

  /**
   *
   * <li>Chaining together calls are at odds with the language syntax;</li>
   * <li>Using an event loop is at odds with the concurrency model;</li>
   */
  public static void virtualThreads2() {
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var user1Future = executor.submit(() -> UserRepository.findUser(0));
      var user2Future = executor.submit(() -> UserRepository.findUser(666));

      var user0 = user1Future.get();
      var user666 = user2Future.get();

      var result = user0.map(User::department).equals(user666.map(User::department));

      printGreen("Virtual Thread result: " + result);
    } catch (IllegalStateException iSEx) {
      printRed("Got an IllegalStateException");
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

   /**
   *
   * <li>Beware of thread leakage;</li>
   * <li>Creating millions of virtual threads is fine ...
   * <ul>... just make sure you keep a reference ...</ul>
   * <ul>... so that you can terminate them, for example;</ul>
   * <ul>... And let Garbage-Collector clear the objects they reference;</ul></li>
   * <li>Use StructuredScope;</li>
   */
  public static void virtualThreads3() {
    var outerThread = Thread.ofVirtual().start(() -> {
      Thread.ofVirtual().start(() ->
          forever(() -> {
            blockingProcess();
            printYellow("Inner thread alive? " + Thread.currentThread().isAlive());
          })
      );

      throw new RuntimeException();
    });

    printRed("Outer thread is alive? " + outerThread.isAlive());

    blockingProcess(1000);

    printRed("Outer thread is alive? " + outerThread.isAlive());

    blockingProcess(100000);
  }

  /**
   *
   * <li>Starts the threads in the scope of the StructuredTaskScope;</li>
   * <li>Shutdowns them as the scope ends;</li>
   * <li>ShutdownOnSuccess waits until the first of the tasks finishes;</li>
   */
  public static void structuredShutdownOnSuccess() {
    try (var scope = new StructuredTaskScope.ShutdownOnSuccess<List<Optional<User>>>()) {
      scope.fork(() -> run(_1k, index -> UserRepository.findUser(index)));
      scope.fork(() -> run(_1k, index -> UserRepository.findUserButReallyFast(index)));

      mainThreadWork();

      var result = scope.join().result(e -> {
        printRed(e.getMessage());
        return e;
      });

      var resultFlattened = result
          .stream()
          .flatMap(Optional::stream)
          .collect(Collectors.toList());

      printGreen(resultFlattened);
    }
  }

  /**
   *
   * Waits for all to finish and then kills the (virtual) threads;
   */
  public static void structuredShutdownOnFailure() {
    try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
      var first = scope.fork(() -> run(0, _5k, index -> UserRepository.findUser(index)));
      var second = scope.fork(() -> run(_5k, _10k, index -> UserRepository.findUserButFlaky(index)));

      // Do some other work
      mainThreadWork();

      scope.join();
      scope.throwIfFailed();

      var allResults = Stream.of(first, second)
          .map(Future::get)
          .flatMap(List::stream)
          .flatMap(Optional::stream)
          .collect(Collectors.toList());

      printGreen(allResults);
    }
  }

  /**
   *
   * <li>ThreadLocals are...</li>
   * <ul>... prone to leaking,</ul>
   * <ul>... and mutable.</ul>
   * </li>
   */
  public static void threadLocal() {
    ThreadLocal<String> threadLocal = new ThreadLocal<>();

    try (var executorService = Executors.newFixedThreadPool(2)) {
      executorService.execute(() -> {
        threadLocal.set("Value set in thread #1");
        printGreen("Thread " + Thread.currentThread().getName() + ": " + threadLocal.get());
        threadLocal.remove();
      });
      executorService.execute(() ->
          printYellow("Thread " + Thread.currentThread().getName() + ": " + threadLocal.get()));
      executorService.execute(() ->
          printRed("Thread " + Thread.currentThread().getName() + ": " + threadLocal.get()));
    }
  }

  /**
   *
   * <li>Scoped values are
   * <ul>... available only within its scope,</ul>
   * <ul>... immutable (but overridable),</ul>
   * <ul>... cheap to inherit;</ul>
   * </li>
   */
  public static void scopedValue() {
    ScopedValue<String> scopedValue = ScopedValue.newInstance();

    try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
      scope.fork(voidCallable(() ->
        ScopedValue.where(scopedValue, "Value set in thread #1", () ->
            printGreen("Thread " + Thread.currentThread() + ": " + scopedValue.orElse("null"))
      )));
      scope.fork(voidCallable(() ->
          printRed("Thread " + Thread.currentThread() + ": " + scopedValue.orElse("null"))
      ));
      scope.fork(voidCallable(() ->
          printYellow("Thread " + Thread.currentThread() + ": " + scopedValue.orElse("null"))
      ));

      scope.join();
    }
  }

  /**
   *
   * <li>Don't block threads from the commonPool;</li>
   * <li>Those should be kept for CPU-bound tasks;</li>
   */
  public static void keyTakeaway1() {
    run(1, 7, __ ->
        CompletableFuture.runAsync(() -> {
          printYellow("Waiting forever on thread " + threadName());
          blockingProcess(Long.MAX_VALUE);
        }));

    var results = IntStream.rangeClosed(0, _10k)
        .parallel()
        .boxed()
        .filter(i -> {
          printYellow("Filtering the stream on thread " + threadName());
          blockingProcess();
          return i % 2 == 0;
        }).collect(Collectors.toList());

    printGreen(results);
  }

  /**
   *
   * <li>Virtual threads are not magically faster;</li>
   * <li>Virtual threads are good at waiting!</li>
   */
  public static void keyTakeaway2() {
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = run(_10k, __ -> executor.submit(Main::cpuHeavyProcess));

      futures.forEach(Future::get);
    }
  }

  /**
   *
   * <li>Be aware of Thread pinning;</li>
   * <li>In some cases Virtual Threads will not unmount from the Platform Threads;
   * <ul>- Synchronized blocks or methods;</ul>
   * <ul>- Native calls;</ul>
   * </li>
   */
  public static void keyTakeaway3() {
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = run(_10k, __ -> executor.submit(new Synchronized()::block));

      futures.forEach(Future::get);
    }
  }

  // EXTRA - ForkJoinPool

  static class FindUserParallel extends RecursiveTask<List<User>> {

    private int threshold = 500;
    private int start;
    private int end;

    public FindUserParallel(int start, int end) {
      this.start = start;
      this.end = end;
    }

    @Override
    protected List<User> compute() {
      if (end - start <= threshold) {
        return run(start, end, UserRepository::findUser)
            .stream()
            .flatMap(Optional::stream)
            .collect(Collectors.toList());
      } else {
        int mid = start + (end - start) / 2;
        var left = new FindUserParallel(start, mid);
        var right = new FindUserParallel(mid, end);
        left.fork();
        var rightResult = right.compute();
        var leftResult = left.join();
        return Stream.concat(rightResult.stream(), leftResult.stream()).collect(Collectors.toList());
      }
    }
  }

  public static void forkJoin() {
    try (var forkJoinPool = new ForkJoinPool(9)) {
      var task = forkJoinPool.submit(new FindUserParallel(0, _10k));

      mainThreadWork();

      var results = task.get();

      printGreen(results);
    }
  }

  // Auxiliary Methods and Fields

  public static final int _1k = 1_000;
  public static final int _5k = _1k * 5;
  public static final int _10k = _5k * 2;


  public static <V> List<V> run(int to, IntFunction<V> callable) {
    return run(0, to, callable);
  }

  public static <V> List<V> run(int from, int to, IntFunction<V> callable) {
    return IntStream.rangeClosed(from, to)
        .mapToObj(callable)
        .collect(Collectors.toList());
  }

  public static class StopWatch {

    private static Instant start;

    public static void start() {
      start = Instant.now();
    }

    public static void stopAndPrint() {
      long milliseconds = Instant.now().toEpochMilli() - start.toEpochMilli();
      System.out.printf("Took %s ms", milliseconds);
    }
  }

  public static void mainThreadWork() {
    // Do some other work
    blockingProcess(1000);
    printGreen("Main thread work done");
  }

  public static void blockingProcess() {
    blockingProcess(1000);
  }

  public static void blockingProcess(long time) {
    Thread.sleep(time);
  }

  public static void cpuHeavyProcess() {
    printYellow("Heavy cpu process running on thread " + threadName());
    Collections.shuffle(IntStream.rangeClosed(0, 1_000_000).boxed().collect(Collectors.toList()));
  }

  public static String threadName() {
    return Thread.currentThread().isVirtual() ? Thread.currentThread().toString() : Thread.currentThread().getName();
  }

  public static void printYellow(Object string) {
    System.out.println("\u001B[33m" + string + "\u001B[0m");
  }

  public static void printRed(Object string) {
    System.out.println("\u001B[31m" + string + "\u001B[0m");
  }

  public static void printGreen(Object string) {
    System.out.println("\u001B[32m" + string + "\u001B[0m");
  }

  public static class Synchronized {

    public synchronized void block() {
      printYellow("Blocked, but in synchronized wait, on thread " + threadName());
      blockingProcess();
    }
  }

  public static void forever(Runnable runnable) {
    while (true) {
      runnable.run();
    }
  }

  public static Callable<Void> voidCallable(Runnable runnable) {
    return () -> {
      runnable.run();
      return null;
    };
  }
}


