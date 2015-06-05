import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by irtaza on 4/29/2015.
 */
class MonitorObjectTwo {

}

class Jobx {
    String ID;
    boolean foundACKED;
    Jobx(String idIN, boolean statusIN){
        ID = idIN;
        foundACKED = statusIN;
    }
}

public class CrackerWorker {

    public static String magic;
    public static String ID = null;
    public static String iPAddress;
    public static String portNumber;
    public static String serverIP;
    public static String serverPort;
    public static DatagramSocket outSock;
    public static DatagramSocket inSock;
    public static volatile String currJobPos;
    public static volatile String currWorkDone;
    public static volatile String currJobID;
    public static volatile String currJobStatus;
    public static volatile String currJobHash;
    public static volatile String currJobRange;
    public static volatile boolean free;
    public static volatile String currJobResult;
    public static volatile ExecutorService crackerService = Executors.newFixedThreadPool(4);
    public static volatile CopyOnWriteArrayList<Jobx> foundJOBSList = new CopyOnWriteArrayList<Jobx>();
    public static volatile int notFoundtracker;
    public static volatile boolean cancelJob = false;
    public static volatile boolean found = false;
    public static volatile boolean jobInProgress = false;
    public static volatile boolean foundAcked = false;
    //public static MonitorObject allocationWait = new MonitorObject();
    public static MonitorObjectTwo jobWait = new MonitorObjectTwo();
    public static void doWait(MonitorObjectTwo input) throws InterruptedException {
        synchronized (input) {
            input.wait();

        }
    }
    public static void doNotify(MonitorObjectTwo input){
        synchronized(input){
            input.notify();
        }
    }

    public static boolean crackPassword(String language,String startPos,String hash,Character startChar) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        int counterOne = language.indexOf(startPos.charAt(0));
        int counterTwo = language.indexOf(startPos.charAt(1));
        int counterThree = language.indexOf(startPos.charAt(2));
        int counterFour = language.indexOf(startPos.charAt(3));
        int counterFive = language.indexOf(startPos.charAt(4));
        System.out.println(counterOne+" "+counterTwo+" "+counterThree+" "+counterFour+" "+counterFive);
        Character first;
        Character second;
        Character third;
        Character fourth;
        Character fifth;
        //  for(int i = counterOne;i<language.length();i++){
        first = startChar;
        System.out.println("Trying Passwords Starting with : "+ startChar);
        for(int j = counterTwo;j<language.length();j++){
            second = language.charAt(j);
            for(int k = counterThree;k<language.length();k++){
                third = language.charAt(k);
                for(int l = counterFour;l<language.length();l++){
                    fourth = language.charAt(l);
                    for(int m = counterFive;m<language.length();m++){
                        fifth = language.charAt(m);
                        String jointPass = new StringBuilder().append(first).append(second).append(third).append(fourth).append(fifth).toString();
                        String tryhash = HashPasswordTwo(jointPass);
                        currWorkDone = jointPass;
                        if(tryhash.equals(hash)){
                            System.out.println("PASSWORD FOUND : " + jointPass);
                            currJobResult = jointPass;
                            return true;
                        }
                        if(cancelJob == true){
                       //     System.out.println("hitting CANCEL JOB==true in Cracker");
                            currJobResult = "CANCELED";
                            return false;
                        }
                        //   System.out.println(jointPass);

                    }
                }
            }
        }
        currJobResult = "NOTFOUND";
     //   System.out.println("RETURNING FROM LOOP END IN CRACKER");
        return  false;
        //    }
    }
    public static boolean crackPasswordTwo(String language,String startPos,String hash,Character startChar) throws NoSuchAlgorithmException, InterruptedException {
     //   notFoundtracker = 0;
        int counterOne = language.indexOf(startPos.charAt(0));
        int counterTwo = language.indexOf(startPos.charAt(1));
        int counterThree = language.indexOf(startPos.charAt(2));
        int counterFour = language.indexOf(startPos.charAt(3));
        int counterFive = language.indexOf(startPos.charAt(4));
        Character first;
        Character second;
        Character third;
        Character fourth;
        Character fifth;
        //  for(int i = counterOne;i<language.length();i++){
        first = startChar;
        for(int j = counterTwo;j<language.length();j++){
            second = language.charAt(j);
            for(int k = counterThree;k<language.length();k++){
                third = language.charAt(k);
                for(int l = counterFour;l<language.length();l++){
                    fourth = language.charAt(l);
                    for(int m = counterFive;m<language.length();m++){
                        fifth = language.charAt(m);
                        String jointPass = new StringBuilder().append(first).append(second).append(third).append(fourth).append(fifth).toString();
                        String tryhash = HashPassword(jointPass);
                        if(tryhash.equals(hash)){
                            System.out.println("PASSWORD FOUND : " + jointPass);

                            found = true;
                            cancelJob = true;
                            currJobStatus = "FOUND";
                            try {
                                Thread.sleep(5000);
                            } catch (InterruptedException e) {

                            }
                            currJobResult = jointPass;

                         //   crackerService.shutdownNow();
                            doNotify(jobWait);
                            return true;
                        }
                        if(cancelJob == true){
                          //  System.out.println("Job Canceled");
                            System.out.println("notFoundtracker is : " + notFoundtracker );
                            if(notFoundtracker == currJobRange.length()){
                                currJobResult = "CANCELED";
                                currJobStatus = "NOTFOUND";
                                doNotify(jobWait);
                            }
                            if(currJobStatus.equals("FOUND")== false) {
                                currJobResult = "CANCELED";
                                currJobStatus = "NOTFOUND";
                            }
                            notFoundtracker++;
                            //notFoundtracker.add(1);
                          //  doNotify(jobWait);

                      //      crackerService.shutdownNow();
                            return false;
                        }

                        //System.out.println(jointPass);

                    }
                }
            }

        }
        if(notFoundtracker == currJobRange.length()){
            currJobResult = "NOTFOUND";
            currJobStatus = "NOTFOUND";
            doNotify(jobWait);
        }
        notFoundtracker++;
        currJobResult = "NOTFOUND";
    //   crackerService.shutdownNow();
        return  false;
        //    }
    }
    public static void jobEnder() throws Exception{
        while(true) {
           // System.out.println("Running Job Ender");
            for (Jobx job : foundJOBSList) {

                if (job.foundACKED == false) {
                    String message = portNumber + "," + iPAddress + "," + magic + "," + ID + "," + "350" + "," + job.ID + "," + currJobStatus + "," + currJobResult;
                    SendMessage(serverPort, serverIP, message);
                    SendMessage(serverPort, serverIP, message);
                }
            }
            Thread.sleep(5000);

        }

    }
    public static void jobHandler(final String currJobHash,final String currJobRange,final String currJobID,final String currJobPos){
        Thread jobHandler = new Thread(new Runnable() {
            @Override
            public void run() {
                jobInProgress = true;
                String alphabets = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
                int startIndex = currJobRange.indexOf(currJobPos.charAt(0));
                boolean notFound = true;
                cancelJob = false;
                String currJobPosone = currJobPos;
                for(int i = startIndex;i<currJobRange.length();i++){
                    try {
                        if (crackPassword(alphabets,currJobPos,currJobHash,currJobRange.charAt(i)) == true) {

                            currJobStatus = "FOUND";
                            notFound = false;
                            Jobx tempjob = new Jobx(currJobID,false);
                            foundJOBSList.add(tempjob);
                           // while(foundAcked == false) {
                             //   String message = portNumber + "," + iPAddress + "," + magic + "," + ID + "," + "350" + "," + currJobID + "," + currJobStatus + "," + currJobResult;

                               // SendMessage(serverPort, serverIP, message);
                            //    Thread.sleep(5000);
                            break;
                        }
                     //   currJobPosone = new StringBuilder().append(alphabets.charAt(startIndex)).append('0').append('0').append('0').append('0').toString();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
                if(notFound == true){
              //      System.out.println("Hitting notFound == true");
                    currJobStatus ="NOTFOUND";
                    String message = portNumber+","+iPAddress+","+magic+","+ID+","+"320"+","+currJobID+","+currJobStatus+","+currJobResult;
                    try {
                        SendMessage(serverPort,serverIP,message);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            jobInProgress = false;
            }
        });
        jobHandler.start();

    }
    public static void jobHandlerTwo(final String currJobHash,final String currJobRange,final String currJobID,final String currJobPos){
        final Thread jobHandler = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("jobHandlerTwo Running");
               final String alphabets = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
                int startIndex = currJobRange.indexOf(currJobPos.charAt(0));
                boolean notFound = true;
                cancelJob = false;
                //crackerService = null;
               // crackerService =  Executors.newFixedThreadPool(4);
                for (int i = 0;i<currJobRange.length();i++) {
                    final char st = currJobRange.charAt(i);
                    crackerService.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                          //      System.out.println("Starting CrackPassword for : " + Thread.currentThread().getName());
                                if(currJobStatus.equals("FOUND")) {
                                    cancelJob = true;
                                }
                                crackPasswordTwo(alphabets, "00000", currJobHash, st);
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    });


                }
                try {
                    doWait(jobWait);
                 //   Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if(currJobStatus.equals("FOUND")){
                    cancelJob = true;
                    String message = portNumber+","+iPAddress+","+magic+","+ID+","+"350"+","+currJobID+","+currJobStatus+","+currJobResult;
                    try {
                        SendMessage(serverPort,serverIP,message);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if(currJobStatus.equals("NOTFOUND")){
                    currJobStatus ="NOTFOUND";
                    String message = portNumber+","+iPAddress+","+magic+","+ID+","+"320"+","+currJobID+","+currJobStatus+","+currJobResult;
                    try {
                        SendMessage(serverPort,serverIP,message);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }


            }
        });
        jobHandler.start();

    }


    public static String recieveMessage() throws IOException {
        byte[] receiveData = new byte[1024];
        byte[] sendData = new byte[1024];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        inSock.receive(receivePacket);
        String sentence = new String(receivePacket.getData(),0,receivePacket.getLength());
        return sentence;
    }
    public static synchronized void SendMessage(String PortNumber,String IP,String message) throws IOException {
        InetAddress IPAddress = InetAddress.getByName(IP);
        byte[] sendData = new byte[1024];
        byte[] receiveData = new byte[1024];
        sendData = message.getBytes();
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.parseInt(PortNumber));
        outSock.send(sendPacket);
        // clientSocket.close();

    }
    public static String HashPassword(String plaintext) throws NoSuchAlgorithmException {
        MessageDigest m = MessageDigest.getInstance("MD5");
        m.reset();
        m.update(plaintext.getBytes());
        byte[] digest = m.digest();
        BigInteger bigInt = new BigInteger(1, digest);
        String hashtext = bigInt.toString(16);
// Now we need to zero pad it if you actually want the full 32 chars.
        while (hashtext.length() < 32) {
            hashtext = "0" + hashtext;
        }
        return hashtext;
    }
    public static String HashPasswordTwo(String plaintext) throws NoSuchAlgorithmException {
        MessageDigest m = MessageDigest.getInstance("MD5");
        m.reset();
        m.update(plaintext.getBytes());
        byte[] digest = m.digest();
        BigInteger bigInt = new BigInteger(1, digest);
        String hashtext = bigInt.toString(16);
// Now we need to zero pad it if you actually want the full 32 chars.

        return hashtext;
    }

    public static String HashPasswordThree(String input) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        //String input = "your string";
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(input.getBytes("UTF-8"));
        byte[] hash = digest.digest();
        String hashtext = hash.toString();
        return hashtext;
    }
    public static void messageHandler() throws IOException, InterruptedException {
        while (true) {
            String message = recieveMessage();

            String [] mV = message.split(",");
            if(mV[4].equals("Ping")!= true){
                System.out.println(message);
            }
            //System.out.println(mV.length);
            if(mV[4].equals("Ping")){
                String reply;
                if(jobInProgress == false) {
                     reply = portNumber + "," + iPAddress + "," + magic + "," + ID + ",PingOk,NULL";
                } else {
                    reply = portNumber + "," + iPAddress + "," + magic + "," + ID + ",PingOk"+","+currWorkDone;

                }
                SendMessage(mV[0],mV[1],reply);
            }

            if(mV[4].equals("200")) {

                ID = mV[3];
            }

            if(mV[4].equals("FIND-ACK")) {
                String tempID = mV[3];
                for(Jobx job : foundJOBSList){
                    if(job.ID.equals(tempID)){
                        job.foundACKED = true;
                    }
                }
             //   foundAcked = true;
              //  Thread.sleep(5000);
            }

            if(mV[4].equals("300")){
             //   currJobPos =
                foundAcked = false;
                cancelJob = false;
                notFoundtracker = 0;
                currJobID = mV[5];
                currJobRange = mV[6];
                currJobHash = mV[7];
                currJobPos = mV[8];
                System.out.println("Job Recieved : ");
                currJobStatus = "INPROGRESS";
                currJobPos = new StringBuilder().append(currJobPos.charAt(0)).append('0').append('0').append('0').append('0').toString();

                jobHandler(currJobHash, currJobRange, currJobID, currJobPos);

            }

            if(mV[4].equals("CANCEL")) {
                cancelJob = true;
            }

        }

    }

//PortNumber,IPaddress,Magic, Client_ID, Command, Key_Range_Start, Key_Range_End, Hash



    public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
        magic = "15440";
        serverPort = "3530";
        serverIP = "192.168.254.1";
        portNumber = args[0];
        inSock = new DatagramSocket(Integer.parseInt(portNumber));
        iPAddress = InetAddress.getLocalHost().getHostAddress();
        outSock = new DatagramSocket();
        Thread ender = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    jobEnder();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        ender.start();
        Thread messageListener = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    messageHandler();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        messageListener.start();
        // Port,IP,Magic,0,2;
        // Send Registration Message to Server
        String Message = portNumber + "," + iPAddress + "," + magic + "," + "0,2,NULL,NULL,";
        SendMessage(serverPort, serverIP, Message);

    }






}
