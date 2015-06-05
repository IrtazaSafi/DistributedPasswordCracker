
import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by irtaza on 4/26/2015.
 */
class Node implements Serializable {
    public String type; // ASSIGN RC or WO
    public String ID;
    public String ipAddress;
    public String portNumber;
    public volatile boolean free;
    public CopyOnWriteArrayList<Job> requestedJobs;
    public Job allocatedJob;
    public volatile int tracker;
    Node(String IDin,String portNumberin,String ipAddressin,String typein) {
        ID = IDin;
        portNumber = portNumberin;
        ipAddress = ipAddressin;
        type = typein;
        requestedJobs = new CopyOnWriteArrayList<Job>();
        tracker = 0;
        free = true;
    }
}

class Job implements Serializable {
    public String status;  // NOTFOUND,FOUND,INPROGRESS
    public String currPos;
    public String result;
    public String jobID;
    public String hash;
    public String range;
    public Node requester;
    public Job parentJob;
    public int notFoundCounter;
    public int cancelCounter;
    public Node assignedSubWorker;
    public CopyOnWriteArrayList<Job> childJobs;
    public ArrayList<Node> assignedWorkers;
    Job(String ID,Node requesterin,String statusIN,String hashin) {
        jobID = ID;
        requester = requesterin;
        status = statusIN;
        assignedWorkers = new ArrayList<Node>();
        childJobs = new CopyOnWriteArrayList<Job>();
        hash = hashin;
    }

}

class MonitorObject implements Serializable {

}

public class CrackerServer implements Serializable {
    CrackerServer(String input){
        portNumber = input;
    }
    public  String portNumber;
    public  String ipAddress;
    public  String magic = "15440";
    public  volatile boolean showMessages = false;
    public transient   DatagramSocket outSock;
    public transient   DatagramSocket inSock;
    public  volatile CopyOnWriteArrayList<Job> globalJobList = new CopyOnWriteArrayList<Job>();
    public  volatile  CopyOnWriteArrayList<Job> globalSubJobList = new CopyOnWriteArrayList<Job>();
   // public  volatile CopyOnWriteArrayList<Job> globalSubJobList = new CopyOnWriteArrayList<Job>();
    public  volatile CopyOnWriteArrayList<Job> executingSubJobList = new CopyOnWriteArrayList<Job>();
    public  volatile CopyOnWriteArrayList<Job> rescheduledList = new CopyOnWriteArrayList<Job>();

    public  volatile CopyOnWriteArrayList<Job> globalScheduledJobList = new CopyOnWriteArrayList<Job>();

    public  volatile ArrayList<Node> connectedWorkers = new ArrayList<Node>();
    public  volatile ArrayList<Node> connectedrequestClients = new ArrayList<Node>();
    public  volatile ArrayList<String> assignedIDs = new ArrayList<String>();
    public  volatile boolean jobInProgress = false;
    public   MonitorObject jobWait = new MonitorObject();

    public   MonitorObject workerWait = new MonitorObject();
    public   MonitorObject workerFreeWait = new MonitorObject();
    public   MonitorObject allocationWait = new MonitorObject();

    public  void doWait(MonitorObject input) throws InterruptedException {
        synchronized (input) {
            input.wait();

        }
    }
    public  void doNotify(MonitorObject input){
        synchronized(input){
            input.notify();
        }
    }


    public  String shuffle(String s) {

        String shuffledString = "";

        while (s.length() != 0)
        {
            int index = (int) Math.floor(Math.random() * s.length());
            char c = s.charAt(index);
            s = s.substring(0,index)+s.substring(index+1);
            shuffledString += c;
        }

        return shuffledString;

    } // Taken From StackOverFlow.com

    public  ArrayList<String> jobCalculater(int numWorkers) {

        if(numWorkers == 0){
            return null;
        }
        numWorkers = numWorkers + 20;
        String alphabets = ("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ");

      //  System.out.println(alphabets.length());
        int dist = alphabets.length() - (alphabets.length() % numWorkers);
        int remainder = alphabets.length() % numWorkers;
        int chunkSize = dist/numWorkers;
     //   System.out.println(chunkSize);
        int track = 0;
        String segment="";
        ArrayList<String> allocations = new ArrayList<String>();
        for(int i = 0;i<alphabets.length()+1;i++) {
            if(track == chunkSize){
                allocations.add(segment);
                track = 0;
                segment ="";
            }
            try {
                segment = segment + alphabets.charAt(i);
            }catch (Exception e) {
                //
            }
            track++;


        }

        String leftover = "";
        for (int i = dist ;i<alphabets.length();i++){
            leftover = leftover + alphabets.charAt(i);
        }
        String x = allocations.get(allocations.size()-1) + leftover;
        allocations.set(allocations.size()-1,x);
        return allocations;
    }


    public  String recieveMessage() throws IOException {
        byte[] receiveData = new byte[1024];
        byte[] sendData = new byte[1024];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        inSock.receive(receivePacket);
      //  System.out.println(receivePacket.getAddress().getHostAddress());
        String sentence = new String(receivePacket.getData(),0,receivePacket.getLength());
        return sentence;
    }
    public  synchronized void SendMessage(String PortNumber,String IP,String message) throws IOException {
        InetAddress IPAddress = InetAddress.getByName(IP);
        byte[] sendData = new byte[1024];
        byte[] receiveData = new byte[1024];
        sendData = message.getBytes();
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, Integer.parseInt(PortNumber));
        outSock.send(sendPacket);
        // clientSocket.close();

    }
    public  String generateID() {
        String answer;
        while(true) {
            Random rand = new Random();
            int temp = rand.nextInt();
            answer = String.valueOf(temp);
            if(assignedIDs.contains(answer) != true) {
                return answer;
            }
        }
    }
    public  Node getNodeByID(String IDin) {
        // Check in RequestClients
        for(int i = 0 ; i< connectedrequestClients.size();i++) {
            if (connectedrequestClients.get(i).ID.equals(IDin)){
                return connectedrequestClients.get(i);
            }
        }
        for(int i = 0 ; i< connectedWorkers.size();i++) {
            if (connectedWorkers.get(i).ID.equals(IDin)){
                return connectedWorkers.get(i);
            }
        }
        return null;
    }

    public  Job getExecutingJobWithID(String ID) {
        for(Job x : executingSubJobList) {
            if(x.jobID.equals(ID)) {
                return x;
            }
        }
        return null;
    }

    public  void pingNodes() throws IOException, InterruptedException {
        while(true) {
            if (connectedrequestClients.size() != 0) {
                for (int i = 0; i < connectedrequestClients.size(); i++) {
                    connectedrequestClients.get(i).tracker++;
                    String tPort = connectedrequestClients.get(i).portNumber;
                    String tIP = connectedrequestClients.get(i).ipAddress;
                    String tID = connectedrequestClients.get(i).ID;
                    String Message = portNumber + "," + ipAddress + "," +magic+","+tID +",Ping";
                    SendMessage(tPort, tIP, Message);
                }
            }

            if (connectedWorkers.size() != 0) {
                for (int i = 0; i < connectedWorkers.size(); i++) {
                    connectedWorkers.get(i).tracker++;
                    String tPort = connectedWorkers.get(i).portNumber;
                    String tIP = connectedWorkers.get(i).ipAddress;
                    String tID = connectedWorkers.get(i).ID;
                    String Message = portNumber + "," + ipAddress +","+magic+ "," + tID + ",Ping";

                    SendMessage(tPort, tIP, Message);
                }
            }
            Thread.sleep(1000);
            if (connectedrequestClients.size() != 0) {
                for (int i = 0; i < connectedrequestClients.size(); i++) {
                    if (connectedrequestClients.get(i).tracker != 0) {
                        Node x = connectedrequestClients.get(i);
                        // Disconnect this Client
                        System.out.println("Request Client at Port " + x.portNumber + " and IP Address " + x.ipAddress + " Disconnected");
                        assignedIDs.remove(connectedrequestClients.get(i).ID);

                        connectedrequestClients.remove(i);
                        if(x.requestedJobs.size()!=0){
                            for(Job job : x.requestedJobs){
                                cancelJob(job);
                            }
                        }
                        // Remove the Jobs Associated with this Client as well. Do this Later.


                    }
                }
            }

            if (connectedWorkers.size() != 0) {
                for (int i = 0; i < connectedWorkers.size(); i++) {
                    Node x = connectedWorkers.get(i);
                    if (connectedWorkers.get(i).tracker != 0) {
                        // Disconnect this Client

                        System.out.println("Worker at " + x.portNumber + " " + x.ipAddress + " Disconnected");
                        assignedIDs.remove(connectedWorkers.get(i).ID);
                        connectedWorkers.remove(i);
                        // Remove the Jobs Associated with this Client as well.
                        if(x.free == false) {
                            executingSubJobList.remove(x.allocatedJob);
                            globalSubJobList.add(0,x.allocatedJob);

                            doNotify(jobWait);
                            doNotify(allocationWait);
                        }
                    }

                }
            }
            Thread.sleep(5000);
        }
        }

    public  void userHandler() {

        while(true) {
            Scanner s = new Scanner(System.in);
            String userInput = s.nextLine();
            final  String [] in = userInput.split(",");
            if (in[0].equals("OFF")) {
                showMessages = false;
                System.err.println("Messages Off");
            }
            if (in[0].equals("ON")) {
                showMessages = true;
                System.err.println("Messages On");
            }
            if(in[0].equals("ShowInfo")){
                System.out.println("Number of Request Clients Connected : " + connectedrequestClients.size());
                System.out.println("Number of Workers Connected : " + connectedWorkers.size());
                System.out.println("Number of Jobs Queued : " + globalJobList.size());
                System.out.println("Number of SubJobs Queued : " + globalSubJobList.size());
                System.out.println("Numbed of Executing SubJobs : " + executingSubJobList.size());

            }

            if(in[0].equals("CLEAR")){
                System.out.println("CLEARING EVERYTHING");
                connectedWorkers.clear();
                connectedrequestClients.clear();
                globalJobList.clear();
                globalSubJobList.clear();
                globalScheduledJobList.clear();
                executingSubJobList.clear();
                assignedIDs.clear();

            }
        }

    }

    public  void jobHandler() throws InterruptedException {

        while(true){
            if (globalJobList.size() == 0){
                System.out.println("Waiting for Job");
                doWait(jobWait);
                continue;
            }
            if (connectedWorkers.size() == 0) {
                System.out.println("Waiting for Workers to Connect");
                doWait(workerWait);
                continue;
            }
          //  if(globalJobList.size()!=0){
                Job newJob = globalJobList.get(0);
                globalScheduledJobList.add(newJob);
                ArrayList<String> allocations = jobCalculater(connectedWorkers.size());
                newJob.notFoundCounter = allocations.size();
                newJob.cancelCounter = allocations.size();
                if(allocations == null){
                 //   System.out.println("allocations is null");
                    continue;
                }
                for (String alloc : allocations){
                    Job tempJob = new Job(generateID(),newJob.requester,"INPROGRESS",newJob.hash);
                    tempJob.range = alloc;
                    tempJob.parentJob = newJob;
                    tempJob.currPos = new StringBuilder().append(alloc.charAt(0)).append('0').append('0').append('0').append('0').toString();

                    globalSubJobList.add(tempJob);
                   // System.out.println(globalSubJobList.size());
                }
                globalJobList.remove(0);
                doNotify(allocationWait);
           // }
        }

    }

    public  boolean workersfree() {
        for(int i = 0;i<connectedWorkers.size();i++){
            if(connectedWorkers.get(i).free == true) {
                return  true;
            }
        }
        return  false;
    }

    public  void cancelJob(Job input) throws IOException {

//        globalJobList.remove(input);
        for(Job job : executingSubJobList){
            if (job.parentJob.equals(input)){
                String cancel = "NULL,NULL,NULL,NULL,CANCEL";
                SendMessage(job.assignedSubWorker.portNumber, job.assignedSubWorker.ipAddress, cancel);
               // globalSubJobList.remove(job);
            }
        }
    }

    public  void jobAssigner() throws IOException, InterruptedException {

        while(true) {
            if(globalSubJobList.size() == 0) {

                doWait(allocationWait);
            }
            if(workersfree() == false){
                System.out.println("Waiting for a Worker to be free");
                doWait(workerFreeWait);
            }

           for (int i = 0 ; i< connectedWorkers.size();i++) {
             //  System.out.println("iterating connected workers");
               if (connectedWorkers.get(i).free == true && globalSubJobList.size()!=0) {
                   Node worker = connectedWorkers.get(i);
                   Job tempJob = globalSubJobList.get(0);
                   tempJob.assignedSubWorker = worker;
                   executingSubJobList.add(tempJob);
                   globalSubJobList.remove(0);
                   worker.allocatedJob = tempJob;
                   worker.free = false;
                   // port,IP,Magic,Client_ID,300,jobID,range,hash
                   String message = portNumber+","+ipAddress+","+magic+","+worker.ID+","+"300"+","+tempJob.jobID+","+tempJob.range
                           +","+tempJob.hash+","+tempJob.currPos;
                   SendMessage(worker.portNumber,worker.ipAddress,message);
               }
           }
        }

    }
// PortNumber,IPAddress,Magic, Client_ID, Command, Key_Range_Start, Key_Range_End, Hash

    public void run() throws Exception {

      //  portNumber = "3530";
        ipAddress = InetAddress.getLocalHost().getHostAddress();
        inSock = new DatagramSocket(Integer.parseInt(portNumber));
        outSock = new DatagramSocket();
        System.out.println("Listening on Port :" + " " + portNumber + " and IP Address " + ipAddress);
        Thread pingThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    pingNodes();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        pingThread.start();
        Thread userListener = new Thread(new Runnable() {
            @Override
            public void run() {
                userHandler();
            }
        });
        userListener.start();

        Thread jobHandler = new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        jobHandler();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        jobHandler.start();
        final Thread jobAssigner = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    jobAssigner();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        jobAssigner.start();

//        Thread serializeThread = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                    while(true){
//                            CrackerServer.saveState(this);
//
//                        try {
//                            Thread.sleep(5000);
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
//                    }
//            }
//        });


        int count = 0;
        while(true) {
            if(count == 1) {
                CrackerServer.saveState(this);
                count = 0;
            }
            String message = recieveMessage();
            count ++;
            if(showMessages == true) {
                System.out.println(message);
            }
            String [] mV = message.split(",");
            if(mV[4].equals("8")){
                // Generate ID , Register Node as Request Client
                if(assignedIDs.contains(mV[3])!=true) {
                    String tempID = generateID();
                    assignedIDs.add(tempID);
                    Node tempNode = new Node(tempID, mV[0], mV[1], "RC");
                    // Generate Job and a Job ID;
                    tempID = generateID();
                    assignedIDs.add(tempID);
                    Job tempJob = new Job(tempID, tempNode, "INPROGRESS", mV[7]);
                    tempNode.requestedJobs.add(tempJob);
                    connectedrequestClients.add(tempNode);
                    globalJobList.add(tempJob);



                    String reply = portNumber + "," + ipAddress + "," + magic + "," + tempNode.ID + ",800"+","+tempJob.jobID+","+"Registered With Server.Job Acknowledged. Wait for  Completion";
                    SendMessage(mV[0], mV[1], reply);
                    System.out.println("Request to Crack  " + tempJob.hash);
                    doNotify(jobWait);
                } else {
                    String tempIDone = mV[3];
                    Node tempNode = getNodeByID(tempIDone);
                    String tempID = generateID();
                    assignedIDs.add(tempID);
                    Job tempJob = new Job(tempID, tempNode, "INPROGRESS", mV[7]);
                    globalJobList.add(tempJob);
                    tempNode.requestedJobs.add(tempJob);
                    String reply = portNumber + "," + ipAddress + "," + magic + "," + tempNode.ID + ",8001,"+tempJob.jobID+","+"Job Acknowledged. Wait for  Completion";
                    SendMessage(mV[0], mV[1], reply);
                    doNotify(jobWait);
                    //    Node x = getNodeByID(tempIDone);
                    //   System.out.println("JOBS for x are  " + x.requestedJobs.size());
                }

            }

            if(mV[4].equals("350")) {
                // Job is Complete, Infrom Requester and cancel other subjobs associated with this Job

                Job tempJob = getExecutingJobWithID(mV[5]);
                if(tempJob == null) continue;
                String parentID = tempJob.parentJob.jobID;
                System.out.println("PARENT ID is"+parentID);
                tempJob.status = mV[6];
                tempJob.result = mV[7];
                String FindAck  = "NULL,NULL,NULL,"+tempJob.jobID+",FIND-ACK";
                SendMessage(mV[0],mV[1],FindAck);
                executingSubJobList.remove(tempJob);
                for(int i = 0;i<executingSubJobList.size();i++){
                    if(executingSubJobList.get(i).parentJob.jobID.equals(parentID)){
                        Node tempWorker = executingSubJobList.get(i).assignedSubWorker;
                        // Cancel Worker working on this SubJob
                        String cancel = "NULL,NULL,NULL,NULL,CANCEL";
                        SendMessage(tempWorker.portNumber,tempWorker.ipAddress,cancel);

                       //  executingSubJobList.remove(i);
                    }
                }
               for(Job job : globalSubJobList){
                   if(job.parentJob.jobID.equals(parentID)){
                       globalSubJobList.remove(job);
                   }
               }
                String goodNews = portNumber+","+ipAddress+","+magic+","+tempJob.parentJob.requester.ID+","+"000"+","+tempJob.parentJob.jobID+","+tempJob.result;
                SendMessage(tempJob.parentJob.requester.portNumber,tempJob.parentJob.requester.ipAddress,goodNews);
                System.out.println("Freeing Worker-COMPLETED");
                getNodeByID(mV[3]).free = true;
                globalJobList.remove(tempJob.parentJob);
                globalScheduledJobList.remove(tempJob.parentJob);
                doNotify(workerFreeWait);


            }

            if(mV[4].equals("320")) {
                if (mV[7].equals("CANCELED")) {
                    Job tempJob = getExecutingJobWithID(mV[5]);
                    if (tempJob.parentJob.cancelCounter == 0) {
                        // Total JOB fINISHED WITH NOT FOUND

                        System.out.println("FREEING WORKER-CANCELED");

                        executingSubJobList.remove(tempJob);
                        globalScheduledJobList.remove(tempJob.parentJob);
                        for(Job job : globalSubJobList){
                            if(job.parentJob.jobID.equals(tempJob.parentJob.jobID)){
                                globalSubJobList.remove(job);
                            }
                        }
                        getNodeByID(mV[3]).free = true;
                        doNotify(workerFreeWait);
                    } else {
                        tempJob.parentJob.cancelCounter--;
                        System.out.println("FREEING WORKER-CANCELED");
                        getNodeByID(mV[3]).free = true;
                        executingSubJobList.remove(tempJob);
                        for(Job job : globalSubJobList){
                            if(job.parentJob.jobID.equals(tempJob.parentJob.jobID)){
                                globalSubJobList.remove(job);
                            }
                        }
                        doNotify(workerFreeWait);
                    }
                }
                if (mV[7].equals("NOTFOUND")) {
                    Job tempJob = getExecutingJobWithID(mV[5]);
                    if(tempJob.parentJob.notFoundCounter == 0){
                        // Total JOB fINISHED WITH NOT FOUND
                        String badNews = portNumber+","+ipAddress+","+magic+","+tempJob.parentJob.requester.ID+","+"111"+","+tempJob.parentJob.jobID+","+tempJob.result;
                        SendMessage(tempJob.parentJob.requester.portNumber,tempJob.parentJob.requester.ipAddress,message);
                        System.out.println("FREEING WORKER-NOTFOUND");
                        getNodeByID(mV[3]).free = true;
                        executingSubJobList.remove(tempJob);
                        globalScheduledJobList.remove(tempJob.parentJob);
                        doNotify(workerFreeWait);


                    } else {
                        tempJob.parentJob.notFoundCounter--;
                        System.out.println("FREEING WORKER-NOTFOUND");
                        getNodeByID(mV[3]).free = true;
                        executingSubJobList.remove(tempJob);
                        doNotify(workerFreeWait);
                    }
                }



            }

            if(mV[4].equals("CANCEL-JOB")){
                Job tempJob=null;
                for (Job job : executingSubJobList ){
                    if(job.parentJob.jobID.equals(mV[5])){
                        tempJob = job.parentJob;
                        System.out.println("ID of temp job is  " + tempJob.jobID);
                        break;
                    }
                }
                cancelJob(tempJob);
            }

            if(mV[4].equals("PingOk")) {
                // System.out.println("mV[3] IS " + mV[3] );
                Node tNode = getNodeByID(mV[3]);
                if(tNode == null){
                    continue;
                }
                tNode.tracker--;

                if(mV[5].equals("NULL") == false){
                    tNode.allocatedJob.currPos = mV[5];
                }
            }

            if(mV[4].equals("2")){
                String tempID = generateID();
                assignedIDs.add(tempID);
                Node tempNode = new Node(tempID, mV[0], mV[1], "WO");
                connectedWorkers.add(tempNode);
                String Message = portNumber + "," + ipAddress + "," + magic + "," +tempNode.ID+",200"+",Worker Registered With Server";
                SendMessage(mV[0],mV[1],Message);
                System.out.println("Worker Registered");
                doNotify(workerWait);
                doNotify(workerFreeWait);
            }






        }

    }

    private static void saveState(CrackerServer crackerServer) throws IOException {
       // System.out.println("SAVING STATE");
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("mymom.ser"));
        oos.writeObject(crackerServer);
        oos.close();

    }

    private static CrackerServer loadState() throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream("mymom.ser"));
        CrackerServer result = (CrackerServer) ois.readObject();
        ois.close();
        return result;
    }



    public static void main(String[] args) throws Exception {

        CrackerServer crackerServer = new CrackerServer(args[0]);
        if(args[1].equals("new")){
         CrackerServer.saveState(crackerServer);
        }
        if (CrackerServer.loadState().equals(null)) {
            System.out.println("NULLL");
            crackerServer = new CrackerServer(args[0]);
        } else {
            crackerServer = CrackerServer.loadState();
        }
        crackerServer.run();
     
}}
