package com.github.bmvisioli;

import static com.github.bmvisioli.Main.blockingProcess;
import static com.github.bmvisioli.Main.printGreen;
import static com.github.bmvisioli.Main.printYellow;
import static com.github.bmvisioli.Main.threadName;

import java.util.List;
import java.util.Optional;

public class UserRepository {

  private static final List<User> users = List.of(
      new User(0, "Bruno", "Java"),
      new User(666, "Brendan", "C#")
  );

  public static Optional<User> findUser(int id) {
    blockingProcess();

    printYellow("Looking for user #" + id + " on thread " + threadName());

    return users.stream().filter(user -> user.id() == id).findFirst();
  }






  public static Optional<User> findUserButReallyFast(int id) {
    blockingProcess(20);

    printGreen("Looking really fast for user #" + id + " on thread " + threadName());

    return users.stream().filter(user -> user.id() == id).findFirst();
  }


  public static Optional<User> findUserButFlaky(int id) {
    var result = findUser(id);
    if (id % 5010 == 0) {
      throw new RuntimeException();
    }
    return result;
  }
}
