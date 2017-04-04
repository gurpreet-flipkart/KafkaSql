package org.kafka.grep.kafka;

import java.io.OutputStream;
import java.io.PrintStream;

public class ThreadPrintStream extends PrintStream {
    private String last = "";
    ThreadLocal<Integer> colorLocal = new ThreadLocal<Integer>() {
        int color = 33;

        @Override
        protected Integer initialValue() {
            if (color == 38) {
                color = 31;
            }
            return color++;
        }
    };

    public ThreadPrintStream(OutputStream out) {
        super(out);
    }

    @Override
    public void println(String msg) {
        if (last.equals(".")) {
            System.out.println();
        }
        last = msg;
        int color = colorLocal.get();
        String s = "[" + color + "m";
        msg = (char) 27 + s + msg + (char) 27 + "[0m";
        super.println(msg);
    }

    @Override
    public void print(String msg) {
        if (last.equals(".") && !msg.equals(".")) {
            System.out.println();
        }
        last = msg;
        int color = colorLocal.get();
        String s = "[" + color + "m";
        msg = (char) 27 + s + msg + (char) 27 + "[0m";
        super.print(msg);
    }
}
