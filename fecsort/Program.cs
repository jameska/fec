using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace fecsort
{
    class Compte
    {
        public string Code;
        public string Intitule;

        public Decimal Debit;
        public Decimal Credit;

        public Compte(string code, string intitule)
        {
            this.Code = code;
            this.Intitule = intitule;

            Debit = Decimal.Zero;
            Credit = Decimal.Zero;
        }

        public void AddMouvement(decimal debit, decimal credit)
        {
            Debit += debit;
            Credit += credit;
        }

    }

    class Journal
    {
        public string Code;
        public string Intitule;

        public Decimal Debit
        {
            get
            {
                decimal solde = decimal.Zero;
                foreach (var cpt in Comptes)
                    solde += cpt.Value.Debit;
                return solde;
            }
        }
        public Decimal Credit
        {
            get
            {
                decimal solde = decimal.Zero;
                foreach (var cpt in Comptes)
                    solde += cpt.Value.Credit;
                return solde;
            }
        }

        public Dictionary<string, Compte> Comptes = new Dictionary<string, Compte>();

        public Journal(string code, string intitule)
        {
            this.Code = code;
            this.Intitule = intitule;
        }

        public void AddMouvement(Compte cpt, decimal debit, decimal credit)
        {
            Compte jalcpt;
            if (!Comptes.TryGetValue(cpt.Code, out jalcpt))
            {
                jalcpt = new Compte(cpt.Code, cpt.Intitule);
                Comptes.Add(jalcpt.Code, jalcpt);
            }
            jalcpt.Debit += debit;
            jalcpt.Credit += credit;
        }

    }

    class ChunkInfo
    {
        public string FileName;
        public StreamReader sr;

        public string[] items;
    }

    class Program
    {
        static Dictionary<string, Action<string>> DispatchArguments = new Dictionary<string, Action<string>>();
        static Dictionary<string, Journal> Journaux = new Dictionary<string, Journal>();
        //static Dictionary<string, EcritureDate> DatesEcriture = new Dictionary<string, EcritureDate>();

        const int MaxChunkSize = 200 * 1000000;
        static EventWaitHandle evtSortDone = new EventWaitHandle(false, EventResetMode.AutoReset);
        static EventWaitHandle evtWriteDone = new EventWaitHandle(false, EventResetMode.AutoReset);

        static string Source;
        static string OutputDir;
        static Encoding encISO = Encoding.GetEncoding("iso-8859-15");

        static long row = 0;
        static long rows = 0;
        static long changes = 0;
        static char[] Separator = new char[] { ';' };
        static string[] Columns;
        static int iCompte;
        static int iCompteLibelle;
        static int iDebit;
        static int iCredit;
        static int iMontant;
        static int iSens;
        static int iEcritureDate;
        static int iEcritureNum;

        static StreamReader GetStreamFromFlatFile()
        {
            return new StreamReader(Source, encISO);
        }
/*
        static StreamReader GetStreamFromZipFile()
        {
            ZipArchive arch = ZipFile.OpenRead(Source);
            ZipArchiveEntry entry;

            entry = arch.Entries[0];

            return new StreamReader(entry.Open(), encISO);
        }

        delegate void delegateExtractDebitCredit(string c1, string c2, out decimal debit, out decimal credit);
        static delegateExtractDebitCredit ExtractDebitCredit;

        static void Extract_DebitCredit(string c1, string c2, out decimal debit, out decimal credit)
        {
            debit = Decimal.Parse(c1);
            credit = Decimal.Parse(c2);
        }

        static void Extract_MontantSens(string c1, string c2, out decimal debit, out decimal credit)
        {
            decimal value = Decimal.Parse(c1);
            if (c2 == "-" || c2 == "C")
            {
                debit = decimal.Zero;
                credit = value;
            }
            else
            {
                debit = value;
                credit = decimal.Zero;
            }
        }
*/
        static void AnalyseHeader(string header)
        {
            string[] itemsPipe = header.Split('|');
            string[] itemsTab = header.Split('\t');
            string[] itemsSemiColon = header.Split(';');

            if (itemsPipe.Length > 2)
                Separator[0] = '|';
            else if (itemsTab.Length > 2)
                Separator[0] = '\t';
            else if (itemsSemiColon.Length > 2)
                Separator[0] = ';';

            Columns = header.Split(Separator);

            iDebit = iCredit = iMontant = iSens = -1;

            foreach (var colname in Columns.Select((a, i) => new { a, i }))
            {
                if ("comptenum".Equals(colname.a, StringComparison.OrdinalIgnoreCase))
                    iCompte = colname.i;
                else if ("comptelib".Equals(colname.a, StringComparison.OrdinalIgnoreCase))
                    iCompteLibelle = colname.i;
                else if ("debit".Equals(colname.a, StringComparison.OrdinalIgnoreCase))
                    iDebit = colname.i;
                else if ("credit".Equals(colname.a, StringComparison.OrdinalIgnoreCase))
                    iCredit = colname.i;
                else if ("montant".Equals(colname.a, StringComparison.OrdinalIgnoreCase))
                    iMontant = colname.i;
                else if ("sens".Equals(colname.a, StringComparison.OrdinalIgnoreCase))
                    iSens = colname.i;
                else if ("ecrituredate".Equals(colname.a, StringComparison.OrdinalIgnoreCase))
                    iEcritureDate = colname.i;
                else if ("ecriturenum".Equals(colname.a, StringComparison.OrdinalIgnoreCase))
                    iEcritureNum = colname.i;
            }
        }

        class LineInfo
        {
            public string[] items;
            public long index;

            public LineInfo(string[] items, long index)
            {
                this.items = items;
                this.index = index;
            }
        }


        static void SortChunk(int num, List<LineInfo> lines)
        {
            evtSortDone.WaitOne();

            ParameterizedThreadStart pts = new ParameterizedThreadStart(doSortChunk);
            Thread t = new Thread(pts);
            t.Start(new Tuple<List<LineInfo>, long>(lines, num));
        }

        static string LongToBase64(long value)
        {
            Byte[] buffer;
            buffer = BitConverter.GetBytes(value);
            if (value < 256 * 256 * 256)
                Array.Resize(ref buffer, 3);
            else if (value < 256L * 256 * 256 * 256 * 256 * 256)
                Array.Resize(ref buffer, 6);
            return Convert.ToBase64String(buffer);
        }

        static long LongFromBase64(string value)
        {
            Byte[] buffer;
            buffer = Convert.FromBase64String(value);
            Array.Resize(ref buffer, 8);
            return BitConverter.ToInt64(buffer,0);
        }

        static void doWriteChunk(object param)
        {
            List<LineInfo> lines = (List<LineInfo>)((Tuple<List<LineInfo>, long>)param).Item1;
            long num = (long)((Tuple<List<LineInfo>, long>)param).Item2;
            try
            {
                using (StreamWriter sw = new StreamWriter(Path.Combine(OutputDir, "chunks", "c" + num + ".txt"), false, encISO))
                {
                    String separator = "" + Separator[0];
                    foreach (var line in lines)
                    {
                        sw.WriteLine(string.Join(separator, line.items) + separator + LongToBase64(line.index));
                    }
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine();
                Console.WriteLine("#error on chunk#{0} - {1}", num, ex.Message);
                Console.ResetColor();
            }

            evtWriteDone.Set();
        }

        static int compareItems(string [] a, string [] b)
        {
            int cmp = a[0].CompareTo(b[0]);
            if (cmp != 0) return cmp;
            cmp = a[iEcritureDate].CompareTo(b[iEcritureDate]);
            if (cmp != 0) return cmp;
            cmp = a[iEcritureNum].CompareTo(b[iEcritureNum]);
            return cmp;
            //if (cmp != 0) return cmp;
            //return (a.index < b.index) ? -1 : (a.index > b.index) ? 1 : 0;
        }

        static int compareChunkInfo(ChunkInfo a, ChunkInfo b)
        {
            int cmp = compareItems(a.items, b.items);
            if (cmp != 0) return cmp;
            long aindex = LongFromBase64(a.items.Last());
            long bindex = LongFromBase64(b.items.Last());
            return aindex < bindex ? -1 : aindex > bindex ? 1 : 0;
        }

        static int compareChunk(LineInfo a, LineInfo b)
        {
            int cmp = compareItems(a.items, b.items);
            if (cmp != 0) return cmp;
            return (a.index < b.index) ? -1 : (a.index > b.index) ? 1 : 0;
        }

        static void doSortChunk(object param)
        {
            List<LineInfo> lines = (List<LineInfo>)((Tuple<List<LineInfo>, long>)param).Item1;
            long num = (long)((Tuple<List<LineInfo>, long>)param).Item2;
            DateTime dtStart = DateTime.Now;
            lines.Sort(compareChunk);
            long ticks = DateTime.Now.Ticks - dtStart.Ticks;
            Console.WriteLine();
            Console.WriteLine("chunk #{0} ({1} rows) Sorted in {2}", num, lines.Count.ToString("N0"), new TimeSpan(ticks));

            ParameterizedThreadStart pts = new ParameterizedThreadStart(doWriteChunk);
            Thread t = new Thread(pts);

            evtWriteDone.WaitOne();

            t.Start( param );

            evtSortDone.Set();
        }
        static void ReadFEC()
        {
            Compte cpt = new Compte("", "");

            Func<StreamReader> GetSource;
            GetSource = GetStreamFromFlatFile;

            string filename = Path.Combine(OutputDir, Path.GetFileNameWithoutExtension(Source) + "~s" + Path.GetExtension(Source));

            List<LineInfo> lines = new List<LineInfo>(3000000);

            Directory.CreateDirectory(Path.Combine(OutputDir, "chunks"));

            int chunkNum = 0;

            using (StreamWriter sw = new StreamWriter(Path.Combine(OutputDir, "result.txt"), false, encISO))
            {
                //                using (StreamReader sr = new StreamReader(Source, encISO))
                using (StreamReader sr = GetSource())
                {
                    string header = sr.ReadLine();

                    AnalyseHeader(header);

                    using (StreamWriter swResult = new StreamWriter(filename, false, encISO))
                    {
                        swResult.WriteLine(header + Separator[0] + "index");
                    }

                    evtSortDone.Set();
                    evtWriteDone.Set();

                    string line;
                    int chunkSize = 0;
                    row = 0;
                    while (null != (line = sr.ReadLine()))
                    {
                        string[] items = line.Split(Separator);

                        row++;

                        if (chunkSize + (line.Length + 2) >= MaxChunkSize)
                        {
                            ++chunkNum;
                            SortChunk(chunkNum, lines);

                            lines = new List<LineInfo>(400000);
                            chunkSize = 0;
                        }

                        chunkSize += (line.Length + 2);
                        lines.Add(new LineInfo(items, row));
                    }
                    ++chunkNum;
                    SortChunk(chunkNum, lines);
                }
            }

            evtSortDone.WaitOne();
            evtWriteDone.WaitOne();

            // merge sorts

            // open chunks
            List<ChunkInfo> chunks = new List<ChunkInfo>(chunkNum);

            for (int num=1; num<=chunkNum; ++num)
            {
                ChunkInfo ci = new ChunkInfo();
                ci.FileName = Path.Combine(OutputDir, "chunks", "c" + num + ".txt");
                ci.sr = new StreamReader(ci.FileName, encISO);
                chunks.Add(ci);
                string line;
                line = ci.sr.ReadLine();
                ci.items = line.Split(Separator);
            }
            //
            try
            {
                chunks.Sort(compareChunkInfo);

                ChunkInfo current = chunks[0];
                chunks.RemoveAt(0);


                rows = row;
                row = 0;

                using (StreamWriter sw = new StreamWriter(filename, true, encISO))
                {
                    string separator = "" + Separator[0];
                    while (current != null)
                    {
                        sw.WriteLine(string.Join(separator, current.items));

                        row++;

                        string line = current.sr.ReadLine();
                        if (line == null)
                        {
                            current.sr.Close();
                            File.Delete(current.FileName);

                            if (chunks.Count > 0)
                            {
                                current = chunks[0];
                                chunks.RemoveAt(0);
                            }
                            else
                                current = null;
                        }
                        else
                        {
                            current.items = line.Split(Separator);
                            if (chunks.Count > 0 && compareChunkInfo(current, chunks[0]) > 0)
                            {
                                ++changes;
                                // insert chunk
                                chunks.Add(current);
                                current = chunks[0];
                                chunks.RemoveAt(0);
                                chunks.Sort(compareChunkInfo);
                            }
                        }
                    }
                }
            }
            finally
            {
                Console.WriteLine();
                Console.WriteLine("suppression des chunks");
                foreach (var chunk in chunks)
                {
                    Console.WriteLine("fermeture de chunk {0}", chunk.FileName);
                    chunk.sr.Close();
                    Console.WriteLine("fermeture de chunk {0}", chunk.FileName);
                    File.Delete(chunk.FileName);
                }
            }
            //
        }

        static bool running = true;
        static void DisplayInformation()
        {
            if (rows > 0)
                Console.Write("rows: {0:N0} / {1:N0}, changes: {2:N0}\r", row, rows, changes);
            else
                Console.Write("rows: {0:N0}\r", row);
        }
        static void DisplayInformationProc()
        {
            while (running)
            {
                DisplayInformation();

                Thread.Sleep(500);
            }
        }

        static void Main(string[] args)
        {
            #region Arguments

            if (args.Length == 0)
            {
                Console.WriteLine("fecbal --source <fecfile> --output <dirpath>");
                Environment.Exit(1);
            }

            DispatchArguments.Add("source", a => Source = a);
            DispatchArguments.Add("output", a => OutputDir = a);

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

            #endregion Arguments

            running = true;
            Thread thread = new Thread(new ThreadStart(DisplayInformationProc));
            thread.Start();

            DateTime dtStart = DateTime.Now;
            try
            {
                ReadFEC();
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
                Console.ResetColor();
                Console.WriteLine(ex.StackTrace);
            }

            DisplayInformation();
            Console.WriteLine();

            DateTime dtEnd = DateTime.Now;
            TimeSpan ts = new TimeSpan(dtEnd.Ticks - dtStart.Ticks);

            Console.WriteLine("temps passé: {0}", ts);

            running = false;
            thread.Join();
#if DEBUG
            Console.ReadKey();
#endif
        }
    }
}
