/*
Name: Rafael Zuniga
Course: CNT 4714 Spring 2025
Assignment title: Project 2 – Multi-threaded programming in Java
Date: February 12, 2025
Class: <name of class goes here>
Description: a description of what the class provides would normally be expected.
*/

package trainYard;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

class TrainYard {
    private static final int MAX_TRAINS = 30;
    private static final Map<String, List<Integer>> yardMap = new HashMap<>();
    static final Map<Integer, SwitchLock> switchLocks = new ConcurrentHashMap<>();
    private static final ExecutorService executor = Executors.newFixedThreadPool(MAX_TRAINS);
    static final List<String> trainStatuses = Collections.synchronizedList(new ArrayList<>());
    private static final List<String> simulationLog = Collections.synchronizedList(new ArrayList<>());

    public static void main(String[] args) {
        log("$ $ $ TRAIN MOVEMENT SIMULATION BEGINS........... $ $ $");
        loadYardFile("theYardFile.csv");
        processFleetFile("TheFleetFile.csv");
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log("$ $ $ SIMULATION ENDS $ $ $");
        writeFinalStatusToFile("FinalTrainStatus.txt");
    }

    private static void loadYardFile(String fileName) {
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length != 5) continue;
                String key = parts[0] + "," + parts[4];
                List<Integer> switches = Arrays.asList(Integer.parseInt(parts[1]),
                                                        Integer.parseInt(parts[2]),
                                                        Integer.parseInt(parts[3]));
                yardMap.put(key, switches);
                switches.forEach(s -> switchLocks.putIfAbsent(s, new SwitchLock(s)));
            }
        } catch (IOException e) {
            log("Error loading yard file: " + e.getMessage());
        }
    }

    private static void processFleetFile(String fileName) {
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length != 3) continue;
                int trainNumber = Integer.parseInt(parts[0]);
                String key = parts[1] + "," + parts[2];
                
                if (!yardMap.containsKey(key)) {
                    log("*************\nTrain " + trainNumber + " is on permanent hold and cannot be dispatched.\n*************");
                    trainStatuses.add("Train " + trainNumber + " | Inbound: " + parts[1] + " | Outbound: " + parts[2] + " | Status: Permanent Hold");
                    continue;
                }
                executor.execute(new Train(trainNumber, yardMap.get(key), parts[1], parts[2]));
            }
        } catch (IOException e) {
            log("Error processing fleet file: " + e.getMessage());
        }
    }

    static void log(String message) {
        System.out.println(message);
        simulationLog.add(message);
    }

    private static void writeFinalStatusToFile(String fileName) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            for (String logEntry : simulationLog) {
                writer.write(logEntry + "\n");
            }
            writer.write("\nFinal Train Status:\n");
            for (String status : trainStatuses) {
                writer.write(status + "\n");
            }
        } catch (IOException e) {
            System.err.println("Error writing final status file: " + e.getMessage());
        }
    }
}

class SwitchLock {
    private final Lock lock = new ReentrantLock();

    public SwitchLock(int switchId) {
    }

    public boolean acquireLock() {
        return lock.tryLock();
    }

    public void releaseLock() {
        lock.unlock();
    }
}

class Train implements Runnable {
    private final int trainNumber;
    private final List<Integer> requiredSwitches;
    private final String inboundTrack;
    private final String outboundTrack;

    public Train(int trainNumber, List<Integer> requiredSwitches, String inboundTrack, String outboundTrack) {
        this.trainNumber = trainNumber;
        this.requiredSwitches = requiredSwitches;
        this.inboundTrack = inboundTrack;
        this.outboundTrack = outboundTrack;
    }

    @Override
    public void run() {
        while (true) {
            TrainYard.log("Train " + trainNumber + ": Attempting to acquire locks " + requiredSwitches);
            if (acquireLocks()) {
                TrainYard.log("Train " + trainNumber + ": HOLDS ALL NEEDED SWITCH LOCKS – Train movement begins.");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                releaseLocks();
                TrainYard.log("@ @ @ TRAIN " + trainNumber + ": DISPATCHED @ @ @");
                TrainYard.trainStatuses.add("Train " + trainNumber + " | Inbound: " + inboundTrack + " | Switches: " + requiredSwitches + " | Outbound: " + outboundTrack + " | Status: Dispatched");
                return;
            }
            TrainYard.log("Train " + trainNumber + " unable to proceed, releasing locks and waiting...");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean acquireLocks() {
        List<Integer> acquiredLocks = new ArrayList<>();
        for (int switchId : requiredSwitches) {
            if (!TrainYard.switchLocks.get(switchId).acquireLock()) {
                releaseLocks(acquiredLocks);
                return false;
            }
            acquiredLocks.add(switchId);
            TrainYard.log("Train " + trainNumber + ": HOLDS LOCK on Switch " + switchId);
        }
        return true;
    }

    private void releaseLocks(List<Integer> acquiredLocks) {
        for (int switchId : acquiredLocks) {
            TrainYard.switchLocks.get(switchId).releaseLock();
            TrainYard.log("Train " + trainNumber + ": Releasing lock on Switch " + switchId);
        }
    }

    private void releaseLocks() {
        for (int switchId : requiredSwitches) {
            TrainYard.switchLocks.get(switchId).releaseLock();
            TrainYard.log("Train " + trainNumber + ": Unlocks/releases lock on Switch " + switchId);
        }
    }
}
