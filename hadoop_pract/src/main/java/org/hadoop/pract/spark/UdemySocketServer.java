package org.hadoop.pract.spark;

import org.apache.commons.io.FileUtils;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;


/**
 * Created by RANJAN on 26-Jun-16.
 */
public class UdemySocketServer {

    public static void main(String[] args) {

        ServerSocket socServer = null;
        String line;
        DataInputStream is;
        PrintStream os;
        Socket clientSocket = null;

        File path = new File("F:\\hadoop\\BDAS-S-Resource-Bundle\\RB-Scala", "streamingtweets.txt");
        List<String> lines = null;
        try {
            lines = FileUtils.readLines(path, "utf-8");
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        try {
            socServer = new ServerSocket(9000);
            System.out.println("Socket opened");

            System.out.println("Total records read :" + lines.size());
        } catch (IOException e) {
            System.out.println(e);
        }

        try {
            clientSocket = socServer.accept();
            System.out.println("Accepted client request from : " + clientSocket.getInetAddress());
            is = new DataInputStream(clientSocket.getInputStream());
            os = new PrintStream(clientSocket.getOutputStream());

            while (true) {

                //Pick a random line
                int randomline = (int) (Math.random() * lines.size());

                System.out.println("Publishing " + lines.get(randomline));
                os.println(lines.get(randomline));
                os.flush();
                //Randomly sleep 1 - 3 seconds
                Thread.sleep((long) (Math.random() * 3000));
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

}
