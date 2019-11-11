using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ofec
{
    class Program
    {
        static Dictionary<string, Action<string>> DispatchArguments = new Dictionary<string, Action<string>>();

        static string Source;
        static string Target;
        static int DuplicateCount = 0;
        static Encoding encISO = Encoding.GetEncoding("iso-8859-15");

        static int time = 0;
        static int row = 0;
        static int maxrow = 0;

        static void DispatchFEC()
        {
            using (StreamWriter sw = new StreamWriter(Target, false, encISO))
            {
                for (int times = 0; times < DuplicateCount; ++times)
                {
                    time = times + 1;
                    using (StreamReader sr = new StreamReader(Source, encISO))
                    {
                        string header = sr.ReadLine();
                        if (times == 0)
                        {
                            sw.WriteLine(header);
                        }
                        string line;
                        row = 0;
                        while (null != (line = sr.ReadLine()))
                        {
#if DEBUG
                            if (row > 1000000)
                                break;
#endif                      
                                ++row;
                            if (maxrow < row)
                                maxrow = row;
                            sw.WriteLine(line);
                        }
                    }
                }
            }
        }

        static bool running = true;
        static void DisplayInformation()
        {
            while (running)
            {
                Console.Write("time: {0} rows: {1:N0} / {2:N0}            \r", time, row, maxrow);

                Thread.Sleep(2000);
            }
        }

        static void Main(string[] args)
        {
            DispatchArguments.Add("source", a => Source = a);
            DispatchArguments.Add("target", a => Target = a);
            DispatchArguments.Add("duplicatecount", a => DuplicateCount = int.Parse(a));

            Action<string> act = null;
            foreach (var arg in args)
            {
                if (act != null)
                {
                    act(arg);
                    act = null;
                    continue;
                }
                if (arg.StartsWith("--"))
                {
                    string argname = arg.Substring(2);
                    DispatchArguments.TryGetValue(argname, out act);
                }
            }

            if (Source != null && File.Exists(Source) &&
                Target != null && DuplicateCount > 0)
            {
                Thread displayThread;

                displayThread = new Thread(new ThreadStart(DisplayInformation));
                displayThread.Start();
                running = true;
                DispatchFEC();
                running = false;

                displayThread.Join();
            }
        }
    }
}
