package com.practice.kafka.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutionException;

public class FileEventSource implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(FileEventSource.class.getName());
    private static final String DELIMITER = ",";

    public boolean keepRunning = true;

    private int updateInterval;
    private File file;
    private long filePointer = 0;
    private EventHandler eventHandler;

    public FileEventSource(int updateInterval, File file, EventHandler eventHandler) {
        this.updateInterval = updateInterval;
        this.file = file;
        this.eventHandler = eventHandler;
    }

    @Override
    public void run() {
        try {
            while (keepRunning) {
                sleep(updateInterval);

                long len = this.file.length();

                if (len < this.filePointer) {
                    log.info("file was reset as filePointer is longer than file length");
                    filePointer = len;
                } else if (len > this.filePointer) {
                    readAppendAndSend();
                }
            }
        } catch (InterruptedException | IOException | ExecutionException e) {
            log.error(e.getMessage());
        }
    }

    private void readAppendAndSend() throws IOException, ExecutionException, InterruptedException {
        RandomAccessFile raf = new RandomAccessFile(this.file, "r");
        raf.seek(this.filePointer);
        String line;
        while ((line = raf.readLine()) != null) {
            sendMessage(line);
        }
        this.filePointer = raf.getFilePointer();
    }

    private void sendMessage(String line) throws ExecutionException, InterruptedException {
        String[] tokens = line.split(DELIMITER);
        String key = tokens[0];
        StringBuilder value = new StringBuilder();

        for (int i = 0; i < tokens.length; i++) {
            if (i != (tokens.length - 1)) {
                value.append(tokens[i]).append(",");
            } else {
                value.append(tokens[i]);
            }
        }

        eventHandler.onMessage(new MessageEvent(key, value.toString()));
    }

    private void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
