using MySql.Data.MySqlClient;
using OpenQA.Selenium;
using OpenQA.Selenium.Chrome;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;

namespace UniversityDataScraper
{
    class Program
    {
        // Static Database Fields
        static string SERVER = "localhost";
        static string DATABASE = "university_db";
        static string USERNAME = "root";
        static string PASSWORD = "root";
        static string connString = "SERVER=" + SERVER + ";DATABASE=" + DATABASE + ";UID=" + USERNAME + ";PASSWORD=" + PASSWORD + ";";

        static void Main(string[] args)
        {
            // Global Fields
            Dictionary<int, string> startLinks = new Dictionary<int, string>();
            List<string> emails = new List<string>();
            List<string> images = new List<string>();
            List<string> pdfs = new List<string>();
            List<string> inLinks = new List<string>();
            List<string> outLinks = new List<string>();
            MySqlConnection mysqlConnection;

            ///************* GET WEBSITES FROM DB *************
            try
            {
                mysqlConnection = new MySqlConnection(connString);
                mysqlConnection.Open();
                string query = "SELECT * FROM university_db.college_directory;";
                MySqlCommand cmd = new MySqlCommand(query, mysqlConnection);
                MySqlDataReader reader = cmd.ExecuteReader();

                while (reader.Read())
                {
                    int id = Int32.Parse(reader["id"].ToString());
                    string link = reader["Website"].ToString();
                    bool isValidLink = isValidUrl(link);
                    if (!id.Equals(null) && !link.Equals(null) && isValidLink)
                    {
                        string finalLink = link;
                        if (finalLink.Contains("http://") || finalLink.Contains("https://") || finalLink.Contains("http://www.") || finalLink.Contains("https://www."))
                        {
                            startLinks.Add(id, finalLink);
                        }
                        else
                        {
                            if (finalLink.Contains("www."))
                            {
                                finalLink = finalLink.Substring(4);
                            }
                            if (finalLink.Contains("http://") || finalLink.Contains("https://"))
                            {
                                startLinks.Add(id, finalLink);
                            }
                            else
                            {
                                finalLink = "http://" + finalLink;
                                startLinks.Add(id, finalLink);
                            }
                        }

                    }
                }
                mysqlConnection.Close();
            }
            catch (MySqlException ex)
            {
                Console.WriteLine("Error! Problem in Database Connection.");
            }
            ///************* END *************

            ///************* Start with Websites *************
            ChromeOptions options = new ChromeOptions();
            options.PageLoadStrategy = PageLoadStrategy.Eager;
            options.AddArgument("--headless");
            IWebDriver driver = new ChromeDriver(@"D:\", options);
            foreach (var link in startLinks)
            {                
                inLinks.Add(link.Value);
                int websiteId = link.Key;

                ///************* CRAWL *************                                            
                for (int i = 0; i < inLinks.Count; i++)
                {
                    // Check the page for inlinks, outlinks, pdfs
                    Uri uri = new Uri(inLinks[i]);
                    driver.Url = inLinks[i];

                    var linkLists = driver.FindElements(By.TagName("a"));
                    if (linkLists.Count > 0)
                    {
                        foreach (var ln in linkLists)
                        {
                            string currentLink = ln.GetAttribute("href");
                            parseLink(ln.GetAttribute("href"), uri, emails, pdfs, inLinks, outLinks);
                        }
                    }

                    // Check the page for images
                    var imgs = driver.FindElements(By.TagName("img"));
                    if (imgs.Count > 0)
                    {
                        foreach (var img in imgs)
                        {
                            string src = img.GetAttribute("src");
                            parseImg(src, images);
                        }
                    }

                    // Check the page for emails
                    extractEmails(driver.PageSource, emails);

                    // Check possible frames
                    var frames = driver.FindElements(By.TagName("frame"));
                    if (frames.Count > 0)
                    {
                        foreach (var frame in frames)
                        {
                            var src = frame.GetAttribute("src");
                            inLinks.Add(src);
                        }
                    }

                }
                ///************* END *************

                ///************* SAVE *************
                if (images.Count > 0)
                    saveCSV("Images", images, websiteId);
                if (pdfs.Count > 0)
                    saveCSV("PDFs", pdfs, websiteId);
                if (inLinks.Count > 0)                
                    saveCSV("LocLinks", inLinks, websiteId);               
                if (outLinks.Count > 0)
                    saveCSV("ExtLinks", outLinks, websiteId);
                if (emails.Count > 0)
                {
                    try
                    {
                        string query = "INSERT INTO `email_directory` (`id`,`email`,`university_id`) VALUES ";
                        int s = 1;
                        foreach (string email in emails)
                        {
                            query = query + "(default,'" + email + "'," + websiteId + "), ";
                            s++;
                        }
                        query = query.Substring(0, query.Length - 2) + ";";
                        mysqlConnection = new MySqlConnection(connString);
                        mysqlConnection.Open();
                        MySqlCommand cmd = new MySqlCommand(query, mysqlConnection);

                        cmd.ExecuteNonQuery();
                        mysqlConnection.Close();
                    }
                    catch (MySqlException ex)
                    {
                        Console.WriteLine("Error! Some of Items have been added previously.");
                    }
                }
                ///************* END *************

                ///************* FREE UP *************
                images = new List<string>();
                pdfs = new List<string>();
                inLinks = new List<string>();
                outLinks = new List<string>();
                emails = new List<string>();
                ///************* END *************
            }
            driver.Close();
            Console.Write("End");
        }

        private static void parseLink(string link, Uri uri, List<string> emailList, List<string> pdfList, List<string> inLinkList, List<string> outLinkList)
        {
            if (link != null)
            {
                if (link.Contains("pdf"))
                {
                    if (!pdfList.Contains(link))
                        pdfList.Add(link);
                }
                else if (link.Contains("doc") || link.Contains("docx") || link.Contains("xls") || link.Contains("xlsx") || link.Contains("txt"))
                {
                    ;
                }
                else if (link.Contains("mailto"))
                {
                    if (!emailList.Contains(link.Substring(7)))
                        emailList.Add(link.Substring(7));
                }
                else if (!link.Contains(uri.Host))
                {
                    if (!outLinkList.Contains(link) && isValidUrl(link))
                        outLinkList.Add(link);
                }
                else
                {
                    if (!inLinkList.Contains(link))
                        inLinkList.Add(link);
                }
            }
        }

        private static void parseImg(string src, List<string> imageList)
        {
            if (!imageList.Contains(src))
                imageList.Add(src);
        }

        private static void saveCSV(string title, List<string> list, int id)
        {
            string folder = @"D:\results\";
            string fileName = title + "_" + id + ".csv";
            string fullPath = folder + fileName;
            list.Insert(0, title);
            string[] items = list.ToArray();    
            File.WriteAllLines(fullPath, items);
        }

        private static bool isValidUrl(string url)
        {
            string pattern = @"^(http|https|ftp|)\://|[a-zA-Z0-9\-\.]+\.[a-zA-Z](:[a-zA-Z0-9]*)?/?([a-zA-Z0-9\-\._\?\,\'/\\\+&amp;%\$#\=~])*[^\.\,\)\(\s]$";
            Regex reg = new Regex(pattern, RegexOptions.Compiled | RegexOptions.IgnoreCase);
            return reg.IsMatch(url);
        }

        private static void extractEmails(string docString, List<string> emailList)
        {
            Regex reg = new Regex(@"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,6}", RegexOptions.IgnoreCase);
            Match match;

            List<string> results = new List<string>();
            for (match = reg.Match(docString); match.Success; match = match.NextMatch())
            {
                if (!(results.Contains(match.Value)))
                    results.Add(match.Value);
            }

            foreach (String email in results)
            {
                emailList.Add(email);
            }
        }
    }
}
