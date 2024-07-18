import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
public class App {
    public static void main(String[] args) throws Exception {
        Class.forName("org.postgresql.Driver");
        crawler c = new crawler();
        c.del();
        c.init();
        c.crawl("https://ru.wikipedia.org/wiki/Link:_The_Faces_of_Evil", 2);
        c.links_count();
    }
}

class crawler {
    private Connection conn;
    crawler()
    {
        try
        {
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/isdatabase", "postgres", "0000");
            conn.setAutoCommit(false);
            System.out.println("nice");
        }
        catch (SQLException e) { System.out.println(e.getMessage()); }
    }
    void init()
    {
        String q = "CREATE TABLE IF NOT EXISTS wordList ("
            + "	rowid SERIAL PRIMARY KEY,"
            + "	word text,"
            + "	isFiltered integer"
            + ");\n"

            + "CREATE TABLE IF NOT EXISTS URLList ("
            + "	rowid SERIAL PRIMARY KEY,"
            + "	URL text"
            + ");\n"

            + "CREATE TABLE IF NOT EXISTS wordLocation ("
            + "	rowid SERIAL PRIMARY KEY,"
            + "	fk_wordId integer,"
            + "	fk_URLId integer,"
            + "	location integer"
            + ");\n"

            + "CREATE TABLE IF NOT EXISTS linkBetweenURL ("
            + "	rowid SERIAL PRIMARY KEY,"
            + "	fk_FromURL_Id integer,"
            + "	fk_ToURL_Id integer"
            + ");\n"

            + "CREATE TABLE IF NOT EXISTS linkWord ("
            + "	rowid SERIAL PRIMARY KEY,"
            + "	fk_wordId integer,"
            + "	fk_linkId integer"
            + ");";
        try
        {
            Statement stmt = conn.createStatement();
            stmt.execute(q);
            conn.commit();
            System.out.println("nice tab");
        }
        catch (SQLException e) { System.out.println(e.getMessage()); }
    }
    void del()
    {
        String q = "DROP TABLE IF EXISTS wordList;\n"
            + "DROP TABLE IF EXISTS URLList;\n"
            + "DROP TABLE IF EXISTS wordLocation;\n"
            + "DROP TABLE IF EXISTS linkBetweenURL;\n"
            + "DROP TABLE IF EXISTS linkWord;\n";
        try
        {
            Statement stmt = conn.createStatement();
            stmt.execute(q);
            conn.commit();
            System.out.println("nice no tab");
        }
        catch (SQLException e) { System.out.println(e.getMessage()); }
    }

    private int getEntryId(String tab, String str, String w)
    {
        String q = "SELECT * FROM " + tab + " WHERE " + w + " = '" + str + "'", q2;
        try
        {
            Statement stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery(q);
            if (res.next())
                return res.getInt("rowid");
            
            if (tab == "URLList")
            {
                q2 = "INSERT INTO URLList (url) VALUES ('" + str + "')";
                stmt.execute(q2);
                conn.commit();
            }
            else if (tab == "wordList")
            {
                q2 = "INSERT INTO wordList (word, isfiltered) VALUES ('" + str + "', 0)";
                stmt.execute(q2);
                conn.commit();
            }
            res = stmt.executeQuery(q);
            res.next();
            return res.getInt("rowid");
        }
        catch (SQLException e)
        {
            System.out.println(e.getMessage() + 9);
            return 1;
        }
    }

    private String separateWords(String str)
    {
        return str.replaceAll("[^а-яА-Яa-zA-Z ]", " ").replaceAll("\\s{2,}", " ").toLowerCase();
    }
    private void addIndex(int url, Document doc)
    {
        int j = -1, num;
        String[] text = separateWords(doc.text()).split(" ");
        for (String str : text)
        {
            if (str.isEmpty())
                continue;
            j++;
            num = getEntryId("wordList", str, "word");

            String q = "INSERT INTO wordLocation (fk_wordId, fk_URLId, location) VALUES ("
                + num + ", " + url + ", " + j + ");";
            try
            {
                Statement stmt = conn.createStatement();
                stmt.execute(q);
                conn.commit();
            }
            catch (SQLException e) { System.out.println(e.getMessage() + 4); }
        }
    }

    private void addLinkRef(int url, int url2, String text)
    {
        int num = 1, num2 = 1;
        String q = "INSERT INTO linkBetweenURL (fk_FromURL_Id, fk_ToURL_Id) VALUES ('"
            + url + "', " + url2 + ");";
        try
        {
            Statement stmt = conn.createStatement();
            stmt.execute(q);
            conn.commit();
        }
        catch (SQLException e) { System.out.println(e.getMessage() + 4); }

        q = "SELECT COUNT(*) FROM linkBetweenURL";
        try
        {
            Statement stmt = conn.createStatement();
            ResultSet res = stmt.executeQuery(q);
            res.next();
            num = res.getInt("count");
        }
        catch (SQLException e) { System.out.println(e.getMessage() + 5); }

        String[] str = separateWords(text).split(" ");
        for (String word : str)
        {
            if (word.isEmpty())
                continue;

            num2 = getEntryId("wordList", word, "word");
            q = "INSERT INTO linkWord (fk_wordId, fk_linkId) VALUES ('"
                + num2 + "', '" + num + "');";
            try
            {
                Statement stmt = conn.createStatement();
                stmt.execute(q);
            }
            catch (SQLException e) { System.out.println(e.getMessage() + 6); }
        }
        try
        {
            conn.commit();
        }
        catch (SQLException e) { System.out.println(7);}
    }
    
    private boolean isIndexed(String url)
    {
        String q = "SELECT fk_urlid FROM wordLocation WHERE fk_urlid = "
            + "(SELECT rowid FROM URLList WHERE url = '" + url + "')";
        try
        {
            Statement stmt = conn.createStatement();
            if (stmt.executeQuery(q).next())
                return true;
            return false;
        }
        catch (SQLException e) { return false; }
    }

    private Map<String, Integer> foundlinks = new HashMap<String, Integer>();
    void crawl(String url, int maxDepth)
    {
        int link = 0;
        Document doc = null;
        ArrayList<String> urllist = new ArrayList<String>(), urllist2 = new ArrayList<String>();
        urllist.add(url);
        getEntryId("URLList", url, "url");
        String[] a = url.split("/");
        String b = a[2];
        foundlinks.put(b, 1);
        for (int i = 0; i < maxDepth; i++)
        {
            for (String susrl : urllist)
            {
                if (isIndexed(susrl))
                    continue;
                link++;
                if (link > 100)
                    return;
                System.out.println("link " + link);
                
                try
                {
                    doc = Jsoup.connect(susrl).userAgent("Chrome/81.0.4044.138").get();
                }
                catch (IOException e) { System.out.println(e.getMessage() + 2); }
                addIndex(link, doc);
                
                Elements elems = doc.select("a");
                for (int j = 0; j < elems.size(); j++)
                {
                    String str = elems.get(j).attr("href");
                    if (str.isEmpty() || str.charAt(0) == '#')
                        continue;

                    if (str.charAt(0) == '/')
                        str = "https://ru.wikipedia.org" + str;
                    if (!str.contains("/") || str.charAt(0) != 'h')
                        continue;

                    if (!str.contains("wikipedia.org"))
                    {
                        a = str.split("/");
                        if (a.length < 3)
                            continue;
                        b = a[2];
                    }
                    else
                        b = "wikipedia.org";
                    if (foundlinks.containsKey(b))
                        foundlinks.put(b, foundlinks.get(b) + 1);
                    else
                        foundlinks.put(b, 1);
                    
                    urllist2.add(str);
                    addLinkRef(link, getEntryId("URLList", str, "url"), elems.get(j).text());
                }
            }
            System.out.println("nice depth " + (i + 1));
            urllist.clear();
            urllist.addAll(urllist2);
            urllist2.clear();
        }
    }
    
    void links_count()
    {
        System.out.println("file");
        try
        {
            PrintWriter f = new PrintWriter(new FileWriter("file.txt"));
            foundlinks.forEach((key, value) -> f.write(key + " " + value + "\n"));
            f.close();
        }
        catch (IOException e) {}
    }
}