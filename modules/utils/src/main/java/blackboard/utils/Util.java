package blackboard.utils;

import java.util.*;
import java.util.regex.*;
import play.Logger;
import play.mvc.Http;
import play.mvc.Call;
import java.security.MessageDigest;

public class Util {
    static final Pattern SLOP = Pattern.compile("~([\\d+])$");
    static final Pattern QUOTE =
        Pattern.compile("([\\+~-])?[\"\\(]([^\"\\)]+)[\"\\)](~[\\d+])?");
    static Pattern TOKEN = Pattern.compile("\\s*([^\\s]+|$)\\s*");

    public static class QueryTokenizer {
        public final List<String> tokens = new ArrayList<>();
        public QueryTokenizer (String text) {
            List<int[]> matches = new ArrayList<>();
            Matcher m = QUOTE.matcher(text);
            while (m.find()) {
                int[] r = new int[]{m.start(), m.end()};
                matches.add(r);
            }
            
            m = TOKEN.matcher(text);
            while (m.find()) {
                int s = m.start();
                int e = m.end();
                boolean valid = true;
                for (int[] r : matches) {
                    if ((s >= r[0] && s < r[1])
                        || (e > r[0] && e < r[1])) {
                        valid = false;
                        break;
                    }
                }
                if (valid && s < e)
                    matches.add(new int[]{s,e});
            }

            Collections.sort(matches, (a, b) -> a[0] - b[0]);
            for (int[] r : matches) {
                tokens.add(text.substring(r[0], r[1]).trim());
            }
        }
        
        public List<String> tokens () { return tokens; }
    }
    
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

    public static String sha1 (Long... values) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA1");
            byte[] buf = new byte[8];
            for (Long v : values) {
                buf[0] = (byte)((v >> 56) & 0xff);
                buf[1] = (byte)((v >> 48) & 0xff);
                buf[2] = (byte)((v >> 40) & 0xff);
                buf[3] = (byte)((v >> 32) & 0xff);
                buf[4] = (byte)((v >> 24) & 0xff);
                buf[5] = (byte)((v >> 16) & 0xff);
                buf[6] = (byte)((v >> 8) & 0xff);
                buf[7] = (byte)(v & 0xff);
                md.update(buf);
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
        if (max <= 9) {
            pages = new int[max];
            for (int i = 0; i < pages.length; ++i)
                pages[i] = i+1;
        }
        else if (page >= max-3) {
            pages = new int[9];
            pages[0] = 1;
            pages[1] = 2;
            pages[2] = 0;
            for (int i = pages.length; --i > 2; )
                pages[i] = max--;
        }
        else {
            pages = new int[9];
            if (page > 4) {
                pages[0] = 1;
                pages[1] = 2;
                pages[2] = 0; // ...
                pages[3] = page-1;
                pages[4] = page;
                pages[5] = page+1;
                pages[6] = 0; // ...
                pages[7] = max-1;
                pages[8] = max;
            }
            else {
                pages[0] = 1;
                pages[1] = 2;
                pages[2] = 3;
                pages[3] = 4;
                pages[4] = 5;
                pages[5] = 6;
                pages[6] = 0;
                pages[7] = max-1;
                pages[8] = max;
            }
        }
        return pages;
    }

    public static String queryRewrite (String query) {
        QueryTokenizer tokenizer = new QueryTokenizer (query);
        StringBuilder q = new StringBuilder ();
        for (String tok : tokenizer.tokens()) {
            if (q.length() > 0)
                q.append(" ");
            char ch = tok.charAt(0);
            if (ch == '+' || ch == '-')
                q.append(tok); // as-is
            else if (ch == '~')
                q.append(tok.substring(1)); // optional token
            else if (ch == '"') {
                int slop = 0;
                Matcher m = SLOP.matcher(tok);
                if (m.find()) {
                    slop = Integer.parseInt(tok.substring(m.start()+1));
                    tok = tok.substring(0, m.start());
                }
                
                q.append("+"+tok);
                if (slop > 0)
                    q.append("~"+slop);
            }
            else {
                // parkinson's => +(parkinson parkinson's parkinsons)
                if (tok.endsWith("'s")) {
                    String s = tok.substring(0, tok.length()-2);
                    tok = "+("+tok+" "+s+" "+s+"s)";
                }
                else if (!"AND".equals(tok)
                         && !"OR".equals(tok) && !"NOT".equals(tok)) {
                    q.append("+");
                }
                q.append(tok);
            }
            //Logger.debug("TOKEN: <<"+tok+">>");
        }
        return q.toString();
    }

    public static String getURL (Call call, Http.Request request) {
        StringBuilder url = new StringBuilder ();
        for (Map.Entry<String, String[]> me
                 : request.queryString().entrySet()) {
            switch (me.getKey()) {
            case "q": case "top": case "skip":
                break; // use reverse routing
            default:
                for (String v : me.getValue())
                    url.append("&" + me.getKey()+"="+v);
            }
        }
        if (call.url().indexOf('?') < 0) {
            url.setCharAt(0, '?');
        }
        return call.url()+url;
    }
}
