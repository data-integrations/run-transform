package test.code.main;

import java.util.Scanner;

public class Runner {

  public static void main(String[] args) {
  String line;
    Scanner stdin = new Scanner(System.in);
    while (stdin.hasNextLine() && !(line = stdin.nextLine()).equals("")) {
        String[] tokens = line.split(" ");
        System.out.println("Hello " + tokens[0] + "...Welcome to the " + tokens[1] + "!!!");
    }
    stdin.close();
  }
  }
