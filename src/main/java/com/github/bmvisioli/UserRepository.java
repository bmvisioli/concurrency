package com.github.bmvisioli;

import static com.github.bmvisioli.Main.blockingProcess;
import static com.github.bmvisioli.Main.cpuHeavyProcess;
import static com.github.bmvisioli.Main.printGreen;
import static com.github.bmvisioli.Main.threadName;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class UserRepository {

  private static final List<User> users = List.of(
      new User(0, "Bruno", "Java"),
      new User(666, "Onurb", "C#")
  );

  public static Optional<User> findUser(int id) {
    blockingProcess();

    printGreen("Looking for user #" + id + " on thread " + threadName());

    return users.stream().filter(user -> user.id() == id).findFirst();
  }

  public static CompletableFuture<Optional<User>> findUserAsync(int id) {
    return CompletableFuture.supplyAsync(() -> findUser(id));
  }
}
