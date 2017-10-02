package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class SimpleDynamoProvider extends ContentProvider {
    static final int SERVER_PORT = 10000;
    static final String TAG = SimpleDynamoActivity.class.getSimpleName();
    boolean flag;
    ArrayList<String> hashedList = new ArrayList<String>(Arrays.asList("177ccecaec32c54b82d5aaafc18a2dadb753e3b1", "208f7f72b198dadd244e61801abe1ec3a4857bc9", "33d6357cfaaf0f72991b0ecd8c56da066613c089", "abf0fd8db03e5ecb199a9b82929e9db79b909643", "c25ddd596aa7c81fa12378fa725f706d54325d12"));
    ArrayList<String> list = new ArrayList<String>(Arrays.asList("5562", "5556", "5554", "5558", "5560"));
    //SQLDatabase sqlDatabase;
    SQLiteDatabase sqLiteDatabase;
    String[] columns = {"KEY", "VALUE"};
    String portStr = null;
    int count = 0;
    boolean waitingFlag;
    boolean insertFlag;
    int counter;


    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        //String node_id = null;
        counter = 0;
        SQLDatabase sqlDatabase = new SQLDatabase(getContext());
        sqLiteDatabase = sqlDatabase.getWritableDatabase();
        //sqlDatabase.onUpgrade(sqLiteDatabase, 0, 1);
        //sqLiteDatabase.delete("Simple_Dynamo2",null,null);
        //waitingFlag = true;
        try {
            TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
            portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
            //node_id = genHash(portStr);
//            if (portStr.equals("5554")) {
            flag = true;
//            } else {
//                flag = false;
//            }
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            System.out.println("Oncreate Joined");
            Message message = new Message();
            message.myPort = portStr;
            message.hashedMyPort = genHash(portStr);
            message.type = "JOIN";
//            message.firstPredecessor = getPredecessor(portStr, 1);
//            message.secondPredecessor = getPredecessor(portStr, 2);
            message.firstSuccessor = getSuccessor(portStr, 1);
            message.secondSuccessor = getSuccessor(portStr, 2);
//            System.out.println("Port: " + portStr + " firstpredecessor: " + message.firstPredecessor + " secondpredecessor: " + message.secondPredecessor + " firstsuccessor: " + message.firstSuccessor
//                    + " secondsuccessor: " + message.secondSuccessor);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
//            }
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Socket socket = null;
        if (selection.equals("\"*\"") || selection.equals("*")) {
            Message message = new Message();
            message.key = null;
            message.type = "*DELETE";
            for (String s : list) {
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(s) * 2);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(message);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("At delete Exception");
                }
            }
        } else if (selection.equals("\"@\"") || selection.equals("@")) {
            SQLDatabase sqlDatabase = new SQLDatabase(getContext());
            SQLiteDatabase sqLiteDatabase = sqlDatabase.getWritableDatabase();
            sqLiteDatabase.delete(SQLDatabase.TABLE_NAME, null, null);
        } else {
            for (int i = 0; i < hashedList.size() - 1; i++) {
                try {
                    if (genHash(selection).compareTo(hashedList.get(i)) > 0 && genHash(selection).compareTo(hashedList.get(i + 1)) <= 0) {
                        for (int j = i + 1; j <= i + 3; j++) {
                            Message message = new Message();
                            message.type = "DELETE";
                            message.key = selection;
                            message.keyHash = genHash(selection);
                            if (j >= hashedList.size()) {
                                message.hashedSenderPort = hashedList.get(j - hashedList.size());
                            } else {
                                message.hashedSenderPort = hashedList.get(j);
                            }
                            //System.out.println("Portif: " + unHashedPort(message.hashedSenderPort) + " Key: " + message.key + " Value: " + message.value);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                        }
                        break;
                    } else if (genHash(selection).compareTo(hashedList.get(hashedList.size() - 1)) > 0 || genHash(selection).compareTo(hashedList.get(0)) == 0 ||
                            genHash(selection).compareTo(hashedList.get(0)) < 0) {
                        for (int j = 0; j <= 2; j++) {
                            Message message = new Message();
                            message.hashedSenderPort = hashedList.get(j);
                            message.type = "DELETE";
                            message.key = selection;
                            message.keyHash = genHash(selection);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                        }
                        break;
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try {
//            while (waitingFlag) {
//
//            }
            String key = values.getAsString("key");
            String value = values.getAsString("value");
            String keyHash = genHash(key);
            for (int i = 0; i < hashedList.size() - 1; i++) {
                if (keyHash.compareTo(hashedList.get(i)) > 0 && keyHash.compareTo(hashedList.get(i + 1)) <= 0) {
                    for (int j = i + 1; j <= i + 3; j++) {
                        Message message = new Message();
                        message.keyHash = keyHash;
                        message.key = key;
                        message.value = value;
                        message.type = "INSERT";
                        if (j >= hashedList.size()) {
                            message.hashedSenderPort = hashedList.get(j - hashedList.size());
                            if (unHashedPort(message.hashedSenderPort).equals("5562") && flag) { //
                                Thread.sleep(2000);
                                flag = false;
                                //waitingFlag=false;
                            }
                        } else {
                            message.hashedSenderPort = hashedList.get(j);
                            if (unHashedPort(message.hashedSenderPort).equals("5562") && flag) { //
                                Thread.sleep(2000);
                                flag = false;
                                //waitingFlag=false;
                            }
                        }
                        System.out.println("Portif: " + unHashedPort(message.hashedSenderPort) + " Key: " + message.key + " Value: " + message.value + " " + message.keyHash);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                    }
                    break;
                } else if (keyHash.compareTo(hashedList.get(hashedList.size() - 1)) > 0 || keyHash.compareTo(hashedList.get(0)) == 0 ||
                        keyHash.compareTo(hashedList.get(0)) < 0) {

                    for (int j = 0; j <= 2; j++) {
                        Message message = new Message();
                        message.keyHash = keyHash;
                        message.key = key;
                        message.value = value;
                        message.type = "INSERT";
                        message.hashedSenderPort = hashedList.get(j);
                        message.type = "INSERT";
                        if (unHashedPort(message.hashedSenderPort).equals("5562") && flag) { //
                            Thread.sleep(2000);
                            flag = false;
                            //waitingFlag=false;
                        }
                        System.out.println("Portelse: " + unHashedPort(message.hashedSenderPort) + " Key: " + message.key + " Value: " + message.value + " " + message.keyHash);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
                    }

                    break;
                }
            }

            Log.v("insert", values.toString());
        } catch (Exception e) {
            Log.e("Insert", "File write failed");
        }
        return uri;
    }


    public String getSuccessor(String port, int i) {
        if (list.indexOf(port) + i < list.size())
            return list.get(list.indexOf(port) + i);
        else
            return list.get(list.indexOf(port) + i - list.size());
    }

    public String getPredecessor(String port, int i) {
        if (list.indexOf(port) - i >= 0)
            return list.get(list.indexOf(port) - i);
        else
            return list.get(list.indexOf(port) - i + list.size());
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub

        MatrixCursor cursor = new MatrixCursor(columns);
        Message message = new Message();
        if (selection.equals("\"*\"") || selection.equals("*")) {
            Socket socket;
            message.list = list;
            message.hashedList = hashedList;
            message.key = null;
            message.type = "*QUERY";
            List<String[]> newList = new ArrayList<String[]>();
            for (String s : message.list) {
                message.senderPort = s;
                try {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(message.senderPort) * 2);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(message);
                    ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
                    List<String[]> list1;
                    list1 = (ArrayList<String[]>) objectInputStream.readObject();
                    newList.addAll(list1);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("At * query");
                }
            }

            for (String[] s : newList) {
                cursor.addRow(new Object[]{s[0], s[1]});
            }

            return cursor;
        } else if (selection.equals("\"@\"") || selection.equals("@")) {

//            while(waitingFlag)
//            {
//
//            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            SQLDatabase sqlDatabase = new SQLDatabase(getContext());
            SQLiteDatabase sqLiteDatabase = sqlDatabase.getReadableDatabase();
            return sqLiteDatabase.query(false, SQLDatabase.TABLE_NAME, columns, null, null, null, null, null, null);
        } else {
            try {
//                while (waitingFlag) {
//
//                }
                Thread.sleep(1000);
                for (int i = 0; i < hashedList.size() - 1; i++) {
                    if (genHash(selection).compareTo(hashedList.get(i)) > 0 && genHash(selection).compareTo(hashedList.get(i + 1)) <= 0) {
                        message.type = "QUERY";
                        message.key = selection;
                        message.keyHash = genHash(selection);
                        message.hashedSenderPort = hashedList.get(i + 1);
                        ArrayList<String[]> keyValues = new ArrayList<String[]>();
                        try {
                            keyValues = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
                            //System.out.print("keyvalues null: "+keyValues.size());
                        } catch (Exception e) {
                            if (i + 2 <= 4) {
                                message.hashedSenderPort = hashedList.get(i + 2);
                            } else {
                                message.hashedSenderPort = hashedList.get(i-3);
                            }
                            keyValues = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
                            if (keyValues==null || keyValues.size() == 0) {
                                if (i + 3 <= 4) {
                                    message.hashedSenderPort = hashedList.get(i + 3);
                                } else {
                                    message.hashedSenderPort = hashedList.get(i-2);
                                }
                                keyValues = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
                            }
                            for (String[] s : keyValues) {
                                System.out.println("Single Exception Query at if: " + s[0] + " " + s[1]);
                                cursor.addRow(new Object[]{s[0], s[1]});
                                System.out.println("In exception" + " " + cursor.getCount());
                            }
                            return cursor;
                        }
                        if (keyValues==null || keyValues.size() == 0)  {
                            if (i + 2 <= 4) {
                                message.hashedSenderPort = hashedList.get(i + 2);
                            } else {
                                message.hashedSenderPort = hashedList.get(i-3);
                            }
                            try {
                                keyValues = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
                            } catch (Exception e) {
                                if (i + 3 <= 4) {
                                    message.hashedSenderPort = hashedList.get(i + 3);
                                } else {
                                    message.hashedSenderPort = hashedList.get(i-2);
                                }
                                keyValues = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
                                for (String[] s : keyValues) {
                                    System.out.println("Single Exception Query at if: " + s[0] + " " + s[1]);
                                    cursor.addRow(new Object[]{s[0], s[1]});
                                    System.out.println("In exception" + " " + cursor.getCount());
                                }
                                return cursor;
                            }
                        }
                        if (keyValues==null || keyValues.size() == 0) {
                            if (i + 3 <= 4) {
                                message.hashedSenderPort = hashedList.get(i + 3);
                            } else {
                                message.hashedSenderPort = hashedList.get(i-2);
                            }
                            try {
                                keyValues = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        for (String[] s : keyValues) {
                            System.out.println("Single Query at if: " + s[0] + " " + s[1]);
                            cursor.addRow(new Object[]{s[0], s[1]});
                        }
                        System.out.println(cursor.getCount());
                        return cursor;
                    } else if (genHash(selection).compareTo(hashedList.get(hashedList.size() - 1)) > 0 || genHash(selection).compareTo(hashedList.get(0)) == 0 ||
                            genHash(selection).compareTo(hashedList.get(0)) < 0) {
                        message.hashedSenderPort = hashedList.get(0);
                        message.type = "QUERY";
                        message.key = selection;
                        message.keyHash = genHash(selection);
                        ArrayList<String[]> keyValues = new ArrayList<String[]>();
                        try {
                            keyValues = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
                        } catch (Exception e) {
                            message.hashedSenderPort = hashedList.get(1);
                            keyValues = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
                            if (keyValues==null || keyValues.size() == 0) {
                                message.hashedSenderPort = hashedList.get(2);
                                keyValues = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
                            }
                            for (String[] s : keyValues) {
                                System.out.println("Single Exception Query at elseif: " + s[0] + " " + s[1]);
                                cursor.addRow(new Object[]{s[0], s[1]});
                                System.out.println("In exception" + " " + cursor.getCount());
                            }
                            return cursor;
                        }
                        if (keyValues==null || keyValues.size() == 0) {
                            message.hashedSenderPort = hashedList.get(1);
                            try {
                                keyValues = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
                            } catch (Exception e) {
                                message.hashedSenderPort = hashedList.get(2);
                                keyValues = (ArrayList<String[]>) new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message).get();
                                for (String[] s : keyValues) {
                                    System.out.println("Single Exception Query at elseif: " + s[0] + " " + s[1]);
                                    cursor.addRow(new Object[]{s[0], s[1]});
                                    System.out.println("In exception" + " " + cursor.getCount());
                                }
                                return cursor;
                            }
                        }
                        for (String[] s : keyValues) {
                            System.out.println("Single Query at elseif: " + s[0] + " " + s[1]);
                            cursor.addRow(new Object[]{s[0], s[1]});
                        }
                        System.out.println(cursor.getCount());
                        return cursor;

                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public String unHashedPort(String hashedPort) {
        try {
            if (genHash("5554").equals(hashedPort)) {
                return "5554";
            } else if (genHash("5556").equals(hashedPort)) {
                return "5556";
            } else if (genHash("5558").equals(hashedPort)) {
                return "5558";
            } else if (genHash("5560").equals(hashedPort)) {
                return "5560";
            } else if (genHash("5562").equals(hashedPort)) {
                return "5562";
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return "";
    }

    private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            while (true) {
                try {
                    Socket socketReceived = serverSocket.accept();
                    ObjectInputStream objectInputStream = new ObjectInputStream(socketReceived.getInputStream());
                    Message message = (Message) objectInputStream.readObject();
                    if (message.type.equals("INSERT")) {
                        insertFlag = true;
                        // System.out.println("At Insert waiting flag: "+waitingFlag);
                        while (waitingFlag) {

                        }
                        //SQLiteDatabase sqLiteDatabase = sqlDatabase.getWritableDatabase();
                        // System.out.println("Server: " + unHashedPort(message.hashedSenderPort));
                        ContentValues values = new ContentValues();
                        values.put(SQLDatabase.KEY, message.key);
                        values.put(SQLDatabase.VALUE, message.value);
                        System.out.println(" At Server Key: " + message.key + ", Value: " + message.value);

                        if (sqLiteDatabase.query(true, SQLDatabase.TABLE_NAME, columns, "KEY='" + message.key + "'", null, null, null, null, null).getCount() != 0) {
//                            System.out.println("Update key: " + message.key + "old value: "
//                                    + sqLiteDatabase.query(true, SQLDatabase.TABLE_NAME, columns, "KEY='" + message.key + "'", null, null, null, null, null).getString(1) +
//                                    " new value: " + message.value);
                            //sqLiteDatabase.delete(SQLDatabase.TABLE_NAME, "KEY='" + message.key + "'",null);
                            int rows = sqLiteDatabase.update(SQLDatabase.TABLE_NAME, values, "KEY='" + message.key + "'", null);
                            System.out.println("Number of rows affected: " + rows);
                        } else {
                            System.out.println("Insert key: " + message.key + " Insert value: " + message.value);
                            long rowid = sqLiteDatabase.insert(SQLDatabase.TABLE_NAME, null, values);
                            System.out.println("Number of row inserted: " + rowid);
                        }

                        //sqLiteDatabase.close();
                        insertFlag = false;
                    } else if (message.type.equals("QUERY") || message.type.equals("*QUERY")) {
                        //System.out.println("At Query waiting flag: "+waitingFlag);

                        while (insertFlag || waitingFlag) {

                        }
                        SQLDatabase sqlDatabase = new SQLDatabase(getContext());
                        SQLiteDatabase sqLiteDatabase = sqlDatabase.getReadableDatabase();

                        Cursor cursor;
                        if (message.type.equals("QUERY")) {
                            cursor = sqLiteDatabase.query(true, SQLDatabase.TABLE_NAME, columns, "KEY='" + message.key + "'", null, null, null, null, null);
                        } else {
                            cursor = sqLiteDatabase.query(true, SQLDatabase.TABLE_NAME, columns, null, null, null, null, null, null);

                        }
                        ArrayList<String[]> keyValues = new ArrayList<String[]>();
                        cursor.moveToFirst();
                        for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
                            String temp[] = new String[2];
                            temp[0] = cursor.getString(cursor.getColumnIndex("key"));
                            temp[1] = cursor.getString(cursor.getColumnIndex("value"));
                            keyValues.add(temp);
                        }

                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketReceived.getOutputStream());
                        objectOutputStream.writeObject(keyValues);

                    } else if (message.type.equals("*DELETE")) {
                        SQLDatabase sqlDatabase = new SQLDatabase(getContext());
                        SQLiteDatabase sqLiteDatabase = sqlDatabase.getWritableDatabase();
                        sqLiteDatabase.delete(SQLDatabase.TABLE_NAME, null, null);
                    } else if (message.type.equals("DELETE")) {
                        SQLDatabase sqlDatabase = new SQLDatabase(getContext());
                        SQLiteDatabase sqLiteDatabase = sqlDatabase.getWritableDatabase();
                        sqLiteDatabase.delete(SQLDatabase.TABLE_NAME, "KEY='" + message.key + "'", null);
                    } else if (message.type.equals(("JOIN"))) {
                        SQLDatabase sqlDatabase = new SQLDatabase(getContext());
                        SQLiteDatabase sqLiteDatabase = sqlDatabase.getReadableDatabase();
                        Cursor cursor;
                        cursor = sqLiteDatabase.query(true, SQLDatabase.TABLE_NAME, columns, null, null, null, null, null, null);
                        System.out.println("At Server: " + message.senderPort);
                        //ArrayList<String[]> keyValues = new ArrayList<String[]>();
                        //ArrayList<String[]> keyValues = new ArrayList<String[]>();
                        if (cursor.getCount() == 0) {
//                            waitingFlag = false;
//                            message.type = "null";
//                            publishProgress(message);
                        } else {
                            message.type = "JOINED";
                            flag = true;
                            cursor.moveToFirst();
                            for (cursor.moveToFirst(); !cursor.isAfterLast(); cursor.moveToNext()) {
                                String temp[] = new String[2];
                                temp[0] = cursor.getString(cursor.getColumnIndex("key"));
                                temp[1] = cursor.getString(cursor.getColumnIndex("value"));
                                String genHashKey = genHash(temp[0]);
                                System.out.println("Key: " + temp[0] + " Value: " + temp[1]);
                                System.out.println("keyhash:" + genHashKey);
                                System.out.println("myporthashmessage:" + message.hashedSenderPort);
                                System.out.println("secondprede:" + genHash(getPredecessor(message.senderPort, 2)));
                                System.out.println("firstprede:" + genHash(getPredecessor(message.senderPort, 1)));
                                System.out.println("myport:" + message.senderPort);
                                System.out.println("myporthash:" + genHash(message.senderPort));
                                System.out.println("Failed Port: " + message.myPort);
                                if (message.firstSuccessor.equals(message.senderPort)) {
                                    System.out.println("check if failed port");
                                    if (getPredecessor(message.senderPort, 1).equals("5562")) {
                                        if (genHashKey.compareTo(genHash("5560")) > 0 || genHashKey.compareTo(genHash("5562")) <= 0) {
                                            System.out.println("Own key");
                                            message.keyValues.add(temp);
                                        }
                                    } else {
                                        if (genHashKey.compareTo(genHash(getPredecessor(message.senderPort, 2))) > 0 && genHashKey.compareTo(genHash(getPredecessor(message.senderPort, 1))) <= 0) {
                                            System.out.println("Own key");
                                            message.keyValues.add(temp);
                                        }
                                    }

                                } else {
                                    System.out.println("check else failed port");
                                    if (message.senderPort.equals("5562")) {
                                        if (genHashKey.compareTo(genHash("5560")) > 0 || genHashKey.compareTo(genHash("5562")) <= 0) {
                                            System.out.println("neighbors keys zero");
                                            message.keyValues.add(temp);
                                        }
                                    } else {
                                        if (genHashKey.compareTo(genHash(getPredecessor(message.senderPort, 1))) > 0 && genHashKey.compareTo(message.hashedSenderPort) <= 0) {
                                            System.out.println("neighbors keys");
                                            message.keyValues.add(temp);
                                        }
                                    }

                                }
                            }
                            publishProgress(message);
                        }
//                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketReceived.getOutputStream());
//                        objectOutputStream.writeObject(keyValues);
                    } else if (message.type.equals("JOINED")) {
                        waitingFlag = true;
                        SQLDatabase sqlDatabase = new SQLDatabase(getContext());
                        SQLiteDatabase sqLiteDatabase = sqlDatabase.getWritableDatabase();
                        System.out.println("Belongs to: " + message.senderPort + " Stored at: " + message.myPort);
                        ContentValues values = new ContentValues();
                        for (String s[] : message.keyValues) {
                            System.out.println("Key: " + s[0] + ", Value: " + s[1] + "Hash of key: " + genHash(s[0]));
                            values.put(SQLDatabase.KEY, s[0]);
                            values.put(SQLDatabase.VALUE, s[1]);
                            if (sqLiteDatabase.query(true, SQLDatabase.TABLE_NAME, columns, "KEY='" + s[0] + "'", null, null, null, null, null).getCount() != 0) {
                                System.out.println("Update key: " + s[0] + " Insert value: " + s[1]);
                                int rows = sqLiteDatabase.update(SQLDatabase.TABLE_NAME, values, "KEY='" + s[0] + "'", null);
                                System.out.println("Number of rows affected: " + rows);
                            } else {
                                System.out.println("Insert key: " + s[0] + " Insert value: " + s[1]);
                                long rowid = sqLiteDatabase.insert(SQLDatabase.TABLE_NAME, null, values);
                                System.out.println("Number of row inserted: " + rowid);
                            }
//                            sqLiteDatabase.insert(SQLDatabase.TABLE_NAME, null, values);
                        }
                    }
                    waitingFlag = false;
//                    } else if (message.type.equals("null")) {
//                        counter+=1;
//                        System.out.println("counter: "+counter);
//                        if (counter == 3 || message.myPort.equals("5562")) {
//                            waitingFlag = false;
//
//                        }
//                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.e(TAG, "File write failed");
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
        }

        protected void onProgressUpdate(Message... message) {
            try {
                if (message[0].type.equals("INSERT REPLY")) {
                    //System.out.println("senderport at publishprigress: " + unHashedPort(message[0].hashedSenderPort));
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message[0]);
                } else if (message[0].type.equals("JOINED")) {
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message[0]);
                }
//                else if (message[0].type.equals("null")) {
//                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message[0]);
//                }
            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }
        }

    }

    private class ClientTask extends AsyncTask<Message, Void, Object> {
        @Override
        protected Object doInBackground(Message... msgs) {
            String port = null;
            try {

                ObjectInputStream objectInputStream;
                Socket socket;
                if (msgs[0].type.equals("INSERT")) {
                    //System.out.println("Client: " + unHashedPort(msgs[0].hashedSenderPort));
                    port = unHashedPort(msgs[0].hashedSenderPort);
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(unHashedPort(msgs[0].hashedSenderPort)) * 2);
                    //socket.setSoTimeout(2000);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(msgs[0]);
                } else if (msgs[0].type.equals("JOIN")) {
                    for (String s : list) {
                        try {
                            if (s.equals(msgs[0].myPort) || s.equals(msgs[0].secondSuccessor)) {
                            } else {
                                msgs[0].senderPort = s;
                                msgs[0].hashedSenderPort = genHash(s);
                                System.out.println("At Client: " + msgs[0].senderPort);
                                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0].senderPort) * 2);
                                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                                objectOutputStream.writeObject(msgs[0]);
                            }
                        } catch (IOException e) {
                            Log.e(TAG, "ClientTask socket IOException");
                            System.out.println("Couldnt find port: " + s);
                            // waitingFlag=false;
                        }
                    }
                } else if (msgs[0].type.equals("JOINED")) {
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0].myPort) * 2);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(msgs[0]);
                }
//                else if (msgs[0].type.equals("null")) {
//                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0].myPort) * 2);
//                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
//                    objectOutputStream.writeObject(msgs[0]);
//                }
                else if (msgs[0].type.equals("INSERT REPLY")) {
//                    msgs[0].senderPort = unHashedPort(msgs[0].hashedSenderPort);
                    for (int i = msgs[0].index; i <= msgs[0].index + 2; i++) {
                        if (i >= hashedList.size()) {
                            msgs[0].hashedSenderPort = hashedList.get(hashedList.size() - i);
                            msgs[0].senderPort = unHashedPort(msgs[0].hashedSenderPort);
                            //System.out.println("senderport at client: " + msgs[0].senderPort + " " + msgs[0].key);
                        } else {
                            msgs[0].hashedSenderPort = hashedList.get(i);
                            msgs[0].senderPort = unHashedPort(msgs[0].hashedSenderPort);
                            //System.out.println("senderport at client: " + msgs[0].senderPort + " " + msgs[0].key);
                        }
                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0].senderPort) * 2);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeObject(msgs[0]);
                    }
                } else if (msgs[0].type.equals("QUERY")) {
                    msgs[0].senderPort = unHashedPort(msgs[0].hashedSenderPort);
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0].senderPort) * 2);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(msgs[0]);
                    objectInputStream = new ObjectInputStream(socket.getInputStream());
                    return objectInputStream.readObject();
                } else if (msgs[0].type.equals("DELETE")) {
                    msgs[0].senderPort = unHashedPort(msgs[0].hashedSenderPort);
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[0].senderPort) * 2);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(msgs[0]);
                }
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                // list.remove(port);
                System.out.println("At IO exception removed port: " + port);
                // System.out.println("List Size after failure: " + list.size());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            return null;

        }
    }
}

class Message implements Serializable {

    String type;
    String senderPort;
    String hashedSenderPort;
    String hashedMyPort;
    String myPort;
    String key;
    String keyHash;
    String value;
    String firstSuccessor;
    String secondSuccessor;
    //    String firstPredecessor;
//    String secondPredecessor;
    int index;
    ArrayList<String> list = new ArrayList<String>();
    ArrayList<String> hashedList = new ArrayList<String>();
    ArrayList<String[]> keyValues = new ArrayList<String[]>();
}


