package com.mapr;
// Java program to count the no. of IP address
// count for successful http response 200 code.
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class LogPArsing {

    public static void findSuccessIpCount(String record) {
        // Creating a regular expression for the records
        final String regex = "^(?!#)([^ ]+)\\\\s+([^ ]+)\\\\s+([^ ]+)\\\\s+([^ ]+)\\\\s+([^ ]+)\\\\s+([^ ]+)\\\\s+([^ ]+)\\\\s+([^ ]+)\\\\s+([^ ]+)\\\\s+([^ ]+)\\\\s+[^\\(]+[\\(]([^\\;]+).*\\%20([^\\/]+)[\\/](.*)$";

        final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);
        final Matcher matcher = pattern.matcher(record);

        System.out.println(record.split(" ")[12]);

        // Creating a Hashmap containing string as
        // the key and integer as the value.
        HashMap<String, Integer> countIP = new HashMap<String, Integer>();
        while (matcher.find()) {

            String IP = matcher.group(1);
            System.out.println("IP :: " + IP);
            String Response = matcher.group(8);
            System.out.println("Response :: " + Response);
            int response = Integer.parseInt(Response);

            // Inserting the IP addresses in the
            // HashMap and maintaining the frequency
            // for each HTTP 200 code.
        }
    }
    public static void main(String[] args)
    {
        final String log = "2018-06-09T07:40:37.627870Z elb123 23.57.74.53:43877 172.31.31.224:80 0.000054 0.135172 0.000082 200 200 74 83 \"POST http://api.superprod.com:80/ws/shout/shoutByUser.json?platform=Cellular&pId=1 HTTP/1.1\" \"Dalvik/2.1.0 (Linux; U; Android 5.1; Lenovo A7010a48 Build/LMY47D)\" - -";

        findSuccessIpCount(log);
    }
}
