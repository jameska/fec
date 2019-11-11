using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace fecbal
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

        public Decimal Debit { get
            {
                decimal solde = decimal.Zero;
                foreach (var cpt in Comptes)
                    solde += cpt.Value.Debit;
                return solde;
            }
        }
        public Decimal Credit { get
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
    class EcritureDate
    {
        public DateTime Date;
        public long Count;
    }


    class Program
    {
        static Dictionary<string, Action<string>> DispatchArguments = new Dictionary<string, Action<string>>();
        static Dictionary<string, Journal> Journaux = new Dictionary<string, Journal>();
        static Dictionary<string, EcritureDate> DatesEcriture = new Dictionary<string, EcritureDate>();

        static string Source;
        static string OutputDir;
        static Encoding encISO = Encoding.GetEncoding("iso-8859-15");

        static long row = 0;
        static char[] Separator = new char[] { ';' };
        static string[] Columns;
        static int iCompte;
        static int iCompteLibelle;
        static int iDebit;
        static int iCredit;
        static int iMontant;
        static int iSens;
        static int iEcritureDate;

        static StreamReader GetStreamFromFlatFile()
        {
            return new StreamReader(Source, encISO);
        }

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
            if (c2=="-" || c2=="C")
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

            foreach (var colname in Columns.Select((a,i) => new { a, i }))
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
            }
            if (iDebit >= 0)
                ExtractDebitCredit = Extract_DebitCredit;
            else
            {
                iDebit = iMontant;
                iCredit = iSens;
                ExtractDebitCredit = Extract_MontantSens;
            }
        }
        static void ReadFEC()
        {
            Compte cpt = new Compte("","");

            Func<StreamReader> GetSource;
            if (Source.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
                GetSource = GetStreamFromZipFile;
            else GetSource = GetStreamFromFlatFile;

            using (StreamWriter sw = new StreamWriter(Path.Combine(OutputDir, "result.txt"), false, encISO))
            {
//                using (StreamReader sr = new StreamReader(Source, encISO))
                using (StreamReader sr = GetSource())
                {
                    string header = sr.ReadLine();

                    AnalyseHeader(header);

                    string line;
                    row = 0;
                    while (null != (line = sr.ReadLine()))
                    {
                        string[] items = line.Split(Separator);
#if DEBUG
                        if (row > 13000000)
                            break;
#endif
                        decimal debit;
                        decimal credit;
                        string journalCode;
                        string journalIntitule;
                        string compteCode;
                        string compteIntitule;

                        journalCode = items[0];
                        journalIntitule = items[1];

                        ExtractDebitCredit(items[iDebit], items[iCredit], out debit, out credit);

                        Journal journal;
                        if (!Journaux.TryGetValue(journalCode, out journal))
                        {
                            journal = new Journal(journalCode, journalIntitule);
                            Journaux.Add(journal.Code, journal);
                        }
                        cpt.Code = items[iCompte];
                        cpt.Intitule = items[iCompteLibelle];
                        journal.AddMouvement(cpt, debit, credit);

                        EcritureDate ecritureDate;
                        if (!DatesEcriture.TryGetValue(items[iEcritureDate], out ecritureDate))
                        {
                            int value = int.Parse(items[iEcritureDate]);
                            ecritureDate = new EcritureDate()
                            {
                                Date = new DateTime(value / 10000, (value / 100) % 100, value % 100),
                                Count = 1
                            };
                            DatesEcriture.Add(items[iEcritureDate], ecritureDate);
                        }
                        else
                            ecritureDate.Count++;

                        ++row;
                    }
                }

                foreach (var jalcode in Journaux.Keys.OrderBy(x => x))
                {
                    Journal jal = Journaux[jalcode];
                    sw.WriteLine(">{0}|{1}|{2:N2}|{3:N2}", jal.Code, jal.Intitule, jal.Debit, jal.Credit);
                    foreach (var jalcpt in jal.Comptes.Values.OrderBy( x => x.Code))
                    {
                        sw.WriteLine("{0}|{1}|{2:N2}|{3:N2}", jalcpt.Code, jalcpt.Intitule, jalcpt.Debit, jalcpt.Credit);
                    }
                }

                sw.WriteLine(">Dates écritures");
                foreach (var date in DatesEcriture.Values.OrderBy(x => x.Date))
                {
                    sw.WriteLine("{0}|{1:N0}", date.Date.ToString("yyyy-MM-dd"), date.Count);
                }
            }
        }

        static bool running = true;
        static void DisplayInformation()
        {
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

            ReadFEC();

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
