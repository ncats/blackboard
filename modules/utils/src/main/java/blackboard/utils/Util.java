package blackboard.utils;

import java.util.*;
import play.Logger;
import play.mvc.Http;
import java.security.MessageDigest;

public class Util {
    private Util () {
    }

    public static String sha1 (Http.Request req, String... params) {
        byte[] sha1 = getSha1 (req, params);
        return sha1 != null ? toHex (sha1) : "";
    }

    public static byte[] getSha1 (Http.Request req, String... params) {
        String path = req.method()+"/"+req.path();
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            md.update(path.getBytes("utf8"));

            Set<String> uparams = new TreeSet<String>();
            if (params != null && params.length > 0) {
                for (String p : params) {
                    uparams.add(p);
                }
            }
            else {
                uparams.addAll(req.queryString().keySet());
            }

            Set<String> sorted = new TreeSet (req.queryString().keySet());
            for (String key : sorted) {
                if (uparams.contains(key)) {
                    String[] values = req.queryString().get(key);
                    if (values != null) {
                        Arrays.sort(values);
                        md.update(key.getBytes("utf8"));
                        for (String v : values)
                            md.update(v.getBytes("utf8"));
                    }
                }
            }

            return md.digest();
        }
        catch (Exception ex) {
            Logger.error("Can't generate hash for request: "+req.uri(), ex);
        }
        return null;
    }

    public static String toHex (byte[] d) {
        StringBuilder sb = new StringBuilder ();
        for (int i = 0; i < d.length; ++i)
            sb.append(String.format("%1$02x", d[i]& 0xff));
        return sb.toString();
    }

    public static String sha1 (String... values) {
        if (values == null)
            return null;
        
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            for (String v : values) {
                md.update(v.getBytes("utf8"));
            }
            return toHex (md.digest());
        }
        catch (Exception ex) {
            Logger.trace("Can't generate sha1 hash!", ex);
        }
        return null;
    }

    public static int[] paging (int rowsPerPage, int page, int total) {
        //last page
        int max = (total+ rowsPerPage-1)/rowsPerPage;
        if (page < 0 || page > max) {
            //throw new IllegalArgumentException ("Bogus page "+page);
            return new int[0];
        }
        
        int[] pages;
        if (max <= 11) {
            pages = new int[max];
            for (int i = 0; i < pages.length; ++i)
                pages[i] = i+1;
        }
        else if (page >= max-3) {
            pages = new int[11];
            pages[0] = 1;
            pages[1] = 2;
            pages[2] = 0;
            for (int i = pages.length; --i > 2; )
                pages[i] = max--;
        }
        else {
            pages = new int[11];
            if (page > 5) {
                pages[0] = 1;
                pages[1] = 2;
                pages[2] = 0; // ...
                pages[3] = page-2;
                pages[4] = page-1;
                pages[5] = page;
                pages[6] = page+1;
                pages[7] = page+2;
                pages[8] = 0; // ...
                pages[9] = max-1;
                pages[10] = max;
            }
            else {
                pages[0] = 1;
                pages[1] = 2;
                pages[2] = 3;
                pages[3] = 4;
                pages[4] = 5;
                pages[5] = 6;
                pages[6] = 7;
                pages[7] = 8;
                pages[8] = 0; // ...
                pages[9] = max-1;
                pages[10] = max;
            }
        }
        return pages;
    }
}
