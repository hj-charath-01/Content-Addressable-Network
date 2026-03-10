package com.can.simulation;

import com.can.core.CANNetwork;
import com.can.core.CANNode;
import com.can.util.HashUtil;

import java.util.Optional;
import java.util.logging.*;
import java.util.Scanner;

/**
 * Demo / test harness for the CAN implementation.
 *
 * Runs through the basic operations: bootstrap, join, store, lookup, leave.
 * Also prints some diagnostics useful for the evaluation section.
 *
 * To run: javac -d out src/.../*.java && java -cp out com.can.simulation.CANSimulation
 *
 * TODO: turn this into proper JUnit tests at some point
 * TODO: add a proper evaluation mode that measures hop counts over different network sizes
 */
public class CANSimulation {

    static {
        // cut down the log spam so output is readable
        Logger root = Logger.getLogger("");
        root.setLevel(Level.WARNING); // change to INFO if you want to see routing details
        for (Handler h : root.getHandlers()) {
            h.setFormatter(new SimpleFormatter() {
                @Override
                public String format(LogRecord r) {
                    return "[" + r.getLevel() + "] " + r.getMessage() + "\n";
                }
            });
        }
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== CAN simulation ===\n");

        // --- setup: 2D network with a handful of nodes ---
        CANNetwork net = new CANNetwork(2);
        CANNode root = net.bootstrap("root");
        System.out.println("bootstrap node ready\n");

        System.out.println("Enter the node names you want in this simulation: (name1, name2, ...., name n)");
        // using real names makes the output easier to read than n1/n2/etc
        Scanner sc = new Scanner (System.in);
        String namesInLine = sc.nextLine();
        String[] nodeNames = namesInLine.trim().split(", ");
        for (String name : nodeNames) {
            CANNode n = net.addNode(name, "root");
            System.out.println("  " + name + " -> " + n.getZone());
        }
        System.out.println();

        // let zone update messages propagate
        Thread.sleep(200);
        net.printSummary();

        // --- store some data ---
        System.out.println("storing data...");
        Object[][] kv = {
            { "user:101",  "alice@example.com" },
            { "user:102",  "bob@example.com" },
            { "user:103",  "carol@example.com" },
            { "config:db", "postgres://prod-db:5432/myapp" },
            { "config:cache", "redis://cache01:6379" },
            { "session:xyzabc", "uid=101&expires=1700000000" },
            { "file:report.pdf", "base64encodedstuffwouldgohere" },
            { "metric:req_per_sec", "847.3" },
        };

        for (Object[] pair : kv) {
            String key = (String) pair[0];
            String val = (String) pair[1];
            boolean ok = net.store(key, val);
            String owner = net.responsibleNode(key).map(CANNode::getNodeId).orElse("??");
            System.out.printf("  %-22s -> %-8s  %s%n", key, owner, ok ? "ok" : "FAILED");
        }
        System.out.println();

        // --- lookups ---
        System.out.println("lookups...");
        String[] lookupKeys = { "user:101", "config:db", "session:xyzabc", "user:999", "nonexistent" };
        for (String key : lookupKeys) {
            Optional<String> result = net.lookup(key);
            if (result.isPresent()) {
                System.out.printf("  HIT  %-22s  \"%s\"%n", key, result.get());
            } else {
                System.out.printf("  miss %-22s%n", key);
            }
        }
        System.out.println();

        // --- partitioning check ---
        System.out.print("checking partitioning... ");
        boolean valid = net.verifyPartitioning();
        System.out.println(valid ? "OK" : "FAILED -- check zone splits!");
        System.out.println();

        // --- per-node metrics ---
        System.out.println("node metrics:");
        net.getAllNodes().stream()
            .sorted(java.util.Comparator.comparing(CANNode::getNodeId))
            .forEach(n -> System.out.println("  " + n.metricsString()));
        System.out.println();

        // --- node departure ---
        System.out.println("bob is leaving...");
        CANNode bob = net.getNode("bob");
        if (bob != null) {
            System.out.println("  bob's zone: " + bob.getZone());
            bob.leave();
            System.out.println("  done. network size: " + net.size());
        }
        Thread.sleep(100);
        net.printSummary();

        // --- verify data is still accessible after departure ---
        System.out.println("checking data availability after bob left...");
        for (Object[] pair : kv) {
            String key = (String) pair[0];
            Optional<String> result = net.lookup(key);
            System.out.printf("  %-22s  %s%n", key,
                result.isPresent() ? "ok (\"" + result.get() + "\")" : "LOST");
        }
        System.out.println();

        // --- key to coordinate mapping (for writeup / debugging) ---
        System.out.println("key -> point mapping:");
        for (Object[] pair : kv) {
            String key = (String) pair[0];
            System.out.printf("  %-22s  sha256=%-18s  -> %s%n",
                key,
                HashUtil.sha256Hex(key).substring(0, 16) + "...",
                HashUtil.hashKey(key, 2));
        }

        // --- quick 3D test ---
        System.out.println("\n3D network test...");
        CANNetwork net3d = new CANNetwork(3);
        net3d.bootstrap("root3d");
        for (int i = 1; i <= 7; i++)
            net3d.addNode("node" + i, "root3d");
        System.out.println("  nodes: " + net3d.size());
        System.out.print("  partitioning: ");
        net3d.verifyPartitioning();

        System.out.println("\ndone.");
    }
}