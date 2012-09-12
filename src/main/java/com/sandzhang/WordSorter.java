package com.sandzhang;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author sand
 */
public class WordSorter {

    private final static int     BLOCK_SIZE                = 4 * 1024;
    private final static int     MAX_WORD                  = 100;

    private static final int     PROCESS_THREAD_COUNT      = 3;
    private static final int     SORT_THREAD_COUNT         = 4;

    private volatile boolean     hasMoreToProcess          = true;

    public volatile long         lastLogTime               = System.currentTimeMillis();
    public volatile long         startTime                 = System.currentTimeMillis();

    private ProcessTask[]        processTasks              = new ProcessTask[PROCESS_THREAD_COUNT];
    private SorterTask[]         sorterTasks               = new SorterTask[SORT_THREAD_COUNT];

    private int                  currentProcessThreadIndex = 0;

    private CountDownLatch       processThreadCountDown    = new CountDownLatch(PROCESS_THREAD_COUNT);
    private CountDownLatch       sortThreadCountDown       = new CountDownLatch(SORT_THREAD_COUNT);
    private AtomicInteger        sortingThreadCount        = new AtomicInteger(0);

    private LinkedList<String[]> sortTaskQueue             = new LinkedList<String[]>();

    private final ReentrantLock  lock                      = new ReentrantLock();

    private char[]               unProcessBuffer           = new char[MAX_WORD];
    private int                  unProcessLength           = 0;

    private Reader               reader;
    private FileWriter           writer;

    private int                  fileTotalLength;

    public WordSorter(String inputFile, String outputFile) throws IOException{
        super();
        reader = new FileReader(inputFile);
        writer = new FileWriter(outputFile);
        for (int i = 0; i < processTasks.length; i++) {
            processTasks[i] = new ProcessTask();
            processTasks[i].start();
        }
        for (int i = 0; i < sorterTasks.length; i++) {
            sorterTasks[i] = new SorterTask();
            sorterTasks[i].start();
        }
    }

    public void process() throws IOException, InterruptedException {
        log("start process...");
        while (processOnce()) {
            while (nextThread().notProcessed) {
            }
        }
        hasMoreToProcess = false;
        processThreadCountDown.await();
        log("process finish");
        sortThreadCountDown.await();
        reader.close();
        writer.close();

        log("total time=" + (System.currentTimeMillis() - startTime));
    }

    private int lastIndexOf(char[] src, char target) {
        for (int i = src.length - 1; i >= 0; i--) {
            if (src[i] == target) return i;
        }
        return src.length - 1;
    }

    private ProcessTask nextThread() {
        currentProcessThreadIndex++;
        if (currentProcessThreadIndex >= processTasks.length) {
            currentProcessThreadIndex = 0;
        }
        return processTasks[currentProcessThreadIndex];
    }

    private boolean processOnce() throws IOException {

        char[] newBuffer = new char[BLOCK_SIZE + unProcessLength];

        if (unProcessLength > 0) {
            System.arraycopy(unProcessBuffer, 0, newBuffer, 0, unProcessLength);
        }

        int readCount = reader.read(newBuffer, unProcessLength, BLOCK_SIZE);
        if (readCount == -1) {
            return false;
        }
        fileTotalLength += readCount;

        int lastIndex = lastIndexOf(newBuffer, '\n');

        ProcessTask currentTask = processTasks[currentProcessThreadIndex];
        // last process is done
        currentTask.buffer = newBuffer;
        currentTask.length = lastIndex;
        currentTask.position = 0;
        // start task
        currentTask.notProcessed = true;

        // cp unProcess
        unProcessLength = readCount + unProcessLength - lastIndex;
        System.arraycopy(newBuffer, lastIndex, unProcessBuffer, 0, unProcessLength);

        return true;
    }

    private void log(String log) {
        long now = System.currentTimeMillis();
        long past = now - lastLogTime;
        lastLogTime = now;
        System.out.println("time=" + past + "       log=" + log);
    }

    public String[] merge(String[] list1, String[] list2, boolean isLast) {
        if (list1 == null) {
            return list2;
        }
        if (list2 == null) {
            return list1;
        }
        int totalArrayLength = list1.length + list2.length;
        String[] tempResult = null;
        StringBuilder writeContent = null;
        if (!isLast) {
            tempResult = new String[totalArrayLength];
        } else {
            writeContent = new StringBuilder(fileTotalLength);
        }
        int list1Pos = 0;
        int list2Pos = 0;

        for (int i = 0; i < totalArrayLength; i++) {
            String word = null;
            if (list1Pos >= list1.length) {
                word = list2[list2Pos++];
            } else if (list2Pos >= list2.length) {
                word = list1[list1Pos++];
            } else if (list1[list1Pos].compareTo(list2[list2Pos]) < 0) {
                word = list1[list1Pos++];
            } else {
                word = list2[list2Pos++];
            }
            if (isLast) {
                writeContent.append(word);
                writeContent.append("\n");
            } else {
                tempResult[i] = word;
            }
        }
        if (isLast) {
            log("write");
            try {
                writer.write(writeContent.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
            log("write");
        }
        return tempResult;
    }

    public void sortQueue() {
        if (sortTaskQueue.size() <= 2) {
            return;
        }
        Collections.sort(sortTaskQueue, new Comparator<String[]>() {

            @Override
            public int compare(String[] o1, String[] o2) {
                return o1.length - o2.length;
            }
        });
    }

    private class SorterTask extends Thread {

        public void run() {
            for (;;) {
                if ((processThreadCountDown.getCount() == 0) && (sortTaskQueue.size() + sortingThreadCount.get() < 2)) {
                    break;
                }

                while (sortTaskQueue.size() >= 2) {
                    String[] list1 = null;
                    String[] list2 = null;
                    boolean isLast = false;

                    if (!lock.tryLock()) continue;
                    try {
                        if (sortTaskQueue.size() < 2) {
                            continue;
                        }
                        sortingThreadCount.incrementAndGet();
                        list1 = sortTaskQueue.poll();
                        list2 = sortTaskQueue.poll();

                        if ((processThreadCountDown.getCount() == 0) && sortingThreadCount.get() == 1
                            && sortTaskQueue.isEmpty()) {
                            isLast = true;
                            log("last sort");
                        }
                    } finally {
                        lock.unlock();
                    }

                    String[] result = merge(list1, list2, isLast);
                    if (!isLast) {
                        lock.lock();
                        try {
                            sortTaskQueue.offer(result);
                            sortQueue();
                        } finally {
                            lock.unlock();
                        }
                    }
                    sortingThreadCount.decrementAndGet();

                }

            }
            sortThreadCountDown.countDown();
        }
    }

    private class ProcessTask extends Thread {

        char[]           buffer;
        int              length;
        int              position;
        volatile boolean notProcessed;

        public void run() {

            while (hasMoreToProcess) {
                if (!notProcessed) {
                    continue;
                }
                SortedStringsList sorter = new SortedStringsList();
                int start = 0;
                int len = 0;
                for (; position < length; position++) {
                    len = position - start;
                    if (buffer[position] == '\n') {
                        if (len >= 1 || (len == 1 && buffer[start] != '\n')) {
                            sorter.add(new String(buffer, start, len));
                        }
                        start = position + 1;
                        len = 0;
                    } else {
                        len++;
                    }
                }
                if (len > 0 && !(len == 1 && buffer[start] != '\n')) {
                    sorter.add(new String(buffer, start, len));
                    start = position + 1;
                    len = 0;
                }

                lock.lock();
                try {
                    sortTaskQueue.offer(sorter.toArray());
                    sortQueue();
                } finally {
                    lock.unlock();
                }
                sorter = new SortedStringsList();
                notProcessed = false;
            }
            processThreadCountDown.countDown();

        }
    }

    public static void main(String args[]) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        new WordSorter("sowpods.txt", "testout" + System.currentTimeMillis() + ".txt").process();
        System.out.printf("cost: %dm \n", System.currentTimeMillis() - start);
    }

}
