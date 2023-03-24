package com.github.bmvisioli;

import java.io.File;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
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

  /*
    Disclaimers:
	  - Superficial demo;
	  - We'll completely ignore Green Threads;
	  - Most here are low-level APIs and very prone to change;
	  - Checked exceptions are suppressed;
   */

  // Find users with an id between 0 and 10k
  public static void main(String[] args) {
    StopWatch.start();

    threads();

    StopWatch.stopAndPrint();
  }

  // Blocks the main thread;
  // Executes commands sequentially (1 by 1);
  // Execution time is the sum of each process execution time (in this case ~ 3h);
  public static void sequential() {
    var results = run(_10k, index -> UserRepository.findUser(index))
        .collect(Collectors.toList());

    // Do some other work...
    mainThreadWork();

    System.out.println(results);
  }

  // Platform Threads are 1-to-1 to OS Threads;
  // OS Threads are expensive (2MB stacktrace);
  // Also context-switching is expensive;
  // Low-level API;
  // Thread-leakage;
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

    System.out.println(results);
  }

  // Executors manage the creation, scheduling and pooling of threads;
  // It uses a pool of threads (in most cases);
  // Each thread is still 1-1 with OS Threads;
  // Execution time is the sum of each process divided by number of threads in the pool;
  public static void executors() {
    try (var executor = Executors.newFixedThreadPool(1000)) {

      var futures = run(_10k, index ->
          executor.submit(() -> UserRepository.findUser(index))
      );

      // Do some other work...
      mainThreadWork();

      var results = futures.map(Future::get)
          .flatMap(Optional::stream)
          .collect(Collectors.toList());

      System.out.println(results);
    }
  }

  // Can be used to form a pipeline;
  // Wraps around threads created by executors;
  // By default uses ForkJoinPool.commonPool;
  // The commonPool size defaults to available CPUs (1 running main thread);
  // Unless another Executor is passed;
  // Function coloring;
  // They have another use;
  public static void completableFutures() {
    var completableFutures = run(_10k, index ->
        CompletableFuture.supplyAsync(() -> UserRepository.findUser(index))
    );

    // Do some other work
    mainThreadWork();

    var results = completableFutures.stream()
        .map(CompletableFuture::get)
        .flatMap(Optional::stream)
        .collect(Collectors.toList());

    System.out.println(results);
  }

  // Virtual threads are cheap because...;
  // ... their stack has a dynamic size ...;
  // ... since it lives in the shared memory (heap);
  // Seamlessly parallelism;
  // Less context-switch;
  // Integrate well wherever Threads and Executors are used;
  // Great for IO-bound (wait) workloads, not for CPU-bound;
  // Shouldn't be pooled;
  public static void virtualThread() {
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = run(_10k, index ->
          executor.submit(() -> UserRepository.findUser(index))
      );

      // Do some other work
      mainThreadWork();

      Thread.sleep(100000);

      var results = futures.stream()
          .map(Future::get)
          .flatMap(Optional::stream)
          .collect(Collectors.toList());

      System.out.println(results);
    }
  }

  // Chaining together calls are at odds with the language syntax
  // Using an event loop is at odds with the concurrency model
  public static void virtualThread2() {
    // Find two users and compare their departments

    // Chaining CompletableFutures

    UserRepository.findUserAsync(0)
        .thenCompose(user0Opt ->
            UserRepository.findUserAsync(666)
                .thenApply(user666Opt -> {
                  throw new RuntimeException();
                  // return user0Opt.map(User::department).equals(user666Opt.map(User::department));
                })
        )
        .exceptionally(e -> {
          if (e instanceof RuntimeException) {
            return false;
          } else if (e instanceof Exception) {
            e.printStackTrace();
            return false;
          } else {
            return true;
          }
        })
        .thenAccept(result -> System.out.println("Future " + result))
        .join();

    /*
      var future = for {
        user0Opt <- UserRepository.findUserAsync(0)
        user666Opt <- UserRepository.findUserAsync(666)
      } yield user0Opt.map(User::department).equals(user666Opt.map(User::department))

      future.

      println(future.get())
     */

    // With Virtual Threads

    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      try {
        var user1Future = executor.submit(() -> UserRepository.findUser(0));
        var user2Future = executor.submit(() -> UserRepository.findUser(666));

        var user0 = user1Future.get();
        var user666 = user2Future.get();

        var result = user0.map(User::department).equals(user666.map(User::department));

        System.out.println("Virtual Thread " + result);
      } catch (RuntimeException rE) {
        System.out.println("RuntimeException: " + rE.getMessage());
      } catch (Exception e) {
        System.out.println("Exception: " + e.getMessage());
      }
    }
  }

  // Starts the threads in the scope of the StructuredTaskScope;
  // Shutdowns them as the scope's end;
  // ShutdownOnSuccess waits until the first of the tasks finishes;
  // Result is available scope.result;
  public static void structuredAny() {
    try (var scope = new StructuredTaskScope.ShutdownOnSuccess<List<Optional<User>>>()) {
      scope.fork(() -> run(0, 1, index -> UserRepository.findUser(index)));
      scope.fork(() -> run(9999, _10k, index -> UserRepository.findUser(index)));
      scope.fork(() -> run(_10k, index -> UserRepository.findUser(index)));

      // Do some other work
      mainThreadWork();

      var result = scope.join().result(e -> {
        System.err.println(e.getMessage());
        return e;
      });

      System.out.println(result);
    }
  }

  // Waits for all to finish and then kills the (virtual) threads;
  // Doesn't have a result method;
  public static void structuredAll() {
    try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
      var first = scope.fork(() -> run(0, 1, index -> UserRepository.findUser(index)));
      var second = scope.fork(() -> run(9999, _10k, index -> UserRepository.findUser(index)));

      // Do some other work
      mainThreadWork();

      scope.join();
      scope.throwIfFailed();

      var allResults = Stream.of(first, second)
          .map(Future::get)
          .flatMap(List::stream)
          .flatMap(Optional::stream)
          .collect(Collectors.toList());

      System.out.println(allResults);
    }
  }

  // Thread Local values

  public static void threadLocal() {
    ThreadLocal<User> activeUser = new InheritableThreadLocal<>();
    ThreadLocal<UUID> sessionId = new InheritableThreadLocal<>();

    Thread.ofPlatform().start(() -> {
      System.out.println("1. Starting outer thread with active user: " + activeUser.get());
      activeUser.set(new User(1, "Bronu", "Java"));
      sessionId.set(UUID.randomUUID());

      Thread.ofPlatform().start(() -> {
        // Child threads will inherit ALL ThreadLocal values

        System.out.println("2. Starting inner thread with active user: " + activeUser.get());
        activeUser.set(new User(2, "Breno", "Java"));

        System.out.println("3. Finished inner thread with active user: " + activeUser.get());
      }).join();

      // ThreadLocal values live until the thread ends (unless .remove())
      System.out.println("4. Finished outer thread with active user: " + activeUser.get());
    }).join();
  }

//  public static void scopedValues() {
//    ScopedValue<User> activeUser = ScopedValue.newInstance();
//
//    ScopedValue.where(activeUser, new User(0, "Bruno", "Java"), () -> {
//      try (var scope = new StructuredTaskScope<>()) {
//
//        scope.fork(() -> {
//          System.out.println("1. Starting outer thread with active user: " + activeUser.get());
//
//          scope.fork(() -> {
//            System.out.println("2. Starting inner thread with active user: " + activeUser.get());
//              ScopedValue.where(activeUser, new User(0, "Breno", "Java"), () -> {
//
//                System.out.println("3. Finished inner thread with active user: " + activeUser.get());
//              }
//          });
//        });
//
//      }
//    });
//  }

  // Takeaways!

  // Don't block threads from the commonPool;
  // Those should be kept for CPU-bound;
  public static void keyTakeaway1() {
    run(ForkJoinPool.getCommonPoolParallelism(), __ ->
        CompletableFuture.runAsync(() -> {
          System.out.println("Waiting forever on thread " + threadName());
          Thread.sleep(Long.MAX_VALUE);
        }));

    var results = IntStream.rangeClosed(0, _10k)
        .parallel()
        .boxed()
        .filter(i -> {
          System.out.println("Filtering the stream on thread " + threadName());
          Thread.sleep(1000);
          return i % 2 == 0;
        }).collect(Collectors.toList());

    System.out.println(results);
  }

  // Virtual threads are not magically faster;
  // They are limited by the resources they use (in this case CPUs);
  // Virtual threads are good at waiting!
  public static void keyTakeaway2() {
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = run(_10k, __ -> executor.submit(Main::cpuHeavyProcess));

      futures.forEach(Future::get);
    }
  }

  // Be aware of Thread pinning;
  // In some cases Virtual Threads will not unmount from the Platform Threads;
  // - Active IO;
  // - Synchronized blocks or methods;
  // - Native calls;
  public static void keyTakeaway3() {
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = run(_10k, __ -> executor.submit(new Synchronized()::block));

      futures.forEach(Future::get);
    }
  }

  // Beware of thread leakage;
  // Creating millions of virtual threads is fine ...
  // ... just make sure you keep a reference ...
  // ... so that you can terminate them, for example;
  // Use StructuredScope;
  public static void keyTakeaway4() {
    var outerThread = Thread.ofVirtual().start(() -> {
      Thread.ofVirtual().start(() -> {
        while (true) {
          Thread.sleep(1000);
          System.out.println("Inner thread alive? " + Thread.currentThread().isAlive());
        }
      });

      throw new RuntimeException();
    });

    System.out.println("Outer thread is alive? " + outerThread.isAlive());

    Thread.sleep(100000);
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

      System.out.println(results);
    }
  }

  public static final int _10k = 10_000;

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
    Thread.sleep(1000);
    printRed("Main thread work done");
  }

  public static void blockingProcess() {
    Thread.sleep(1000);
  }

  public static void cpuHeavyProcess() {
    System.out.println("Heavy cpu process running on thread " + threadName());
    Collections.shuffle(IntStream.rangeClosed(0, 1_000_000).boxed().collect(Collectors.toList()));
  }

  public static String threadName() {
    return Thread.currentThread().isVirtual() ? "Virtual - " + Thread.currentThread().threadId() : Thread.currentThread().getName();
  }

  public static void printGreen(String string) {
    System.out.println("\u001B[32m" + string);
  }

  public static void printRed(String string) {
    System.out.println("\u001B[33m" + string);
  }

  public static class Synchronized {
    public synchronized void block() {
      System.out.println("Blocked, but in synchronized wait, on thread " + threadName());
      blockingProcess();
    }
  }
}


