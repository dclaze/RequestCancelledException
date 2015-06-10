using System;
using System.IO;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Smuggler;

namespace RequestCancelledException
{
    class Program
    {
        static void Main(string[] args)
        {
            const string databaseName = "DatabaseForRequestCancelledExceptionTest";
            const string ravenUrl = "http://localhost:8080";

            const string backUpFolder = @"C:\Backups";
            CreateBackUpDirectoryIfNotExist(backUpFolder);

            var filePath = Path.Combine(backUpFolder, string.Format("{0}.ravendump", databaseName));
            var options = new SmugglerOptions()
            {
                BackupPath = filePath,
                OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Transformers,
                BatchSize = 4096
            };
            var connectionStringOptions = new RavenConnectionStringOptions()
            {
                Url = ravenUrl,
                DefaultDatabase = databaseName
            };

            EnsureDatabaseExists(ravenUrl, databaseName);
            DeleteAllTestDatabaseDocuments(ravenUrl, databaseName);

            //Test 1 - Number of documents == Batch size. Seems to work.
            RunTest(ravenUrl, databaseName, options, connectionStringOptions, 1);

            //Test 2 - Number of documents == Batch size x 2. Seems to work.
            RunTest(ravenUrl, databaseName, options, connectionStringOptions, 2);

            //Test 5 - Number of documents == Batch size x 10. Seems to break here
            RunTest(ravenUrl, databaseName, options, connectionStringOptions, 10);

            Console.WriteLine("Exception should happen before you can read this!!! If the program executes successfully try adding more documents to the database, by adjusting the multiplier above.");
            Console.ReadKey();
        }

        private static void EnsureDatabaseExists(string ravenUrl, string databaseName)
        {
            var documentStore = new DocumentStore { Url = ravenUrl }.Initialize();
            documentStore.DatabaseCommands.EnsureDatabaseExists(databaseName);
        }

        private static void CreateBackUpDirectoryIfNotExist(string backUpFolder)
        {
            if (!Directory.Exists(backUpFolder))
                Directory.CreateDirectory(backUpFolder);
        }

        private static void RunTest(string ravenUrl, string databaseName, SmugglerOptions options, RavenConnectionStringOptions connectionStringOptions, int batchSizeMultiplier)
        {
            LoadDatabaseWithTestDocuments(ravenUrl, databaseName, options.BatchSize * batchSizeMultiplier);
            Export(options, connectionStringOptions);
            Import(options, connectionStringOptions);
            DeleteAllTestDatabaseDocuments(ravenUrl, databaseName);
        }

        private static void Export(SmugglerOptions options, RavenConnectionStringOptions connectionStringOptions)
        {
            var smugglerApi = new SmugglerApi(options, connectionStringOptions);
            using (var importTask = smugglerApi.ExportData(null, options, false))
            {
                importTask.Wait();
            }
        }

        private static void Import(SmugglerOptions options, RavenConnectionStringOptions connectionStringOptions)
        {
            var smugglerApi = new SmugglerApi(options, connectionStringOptions);
            using (var importTask = smugglerApi.ImportData(options))
            {
                importTask.Wait();
            }
        }

        private static void LoadDatabaseWithTestDocuments(string ravenUrl, string databaseName, int numberOfDocuments)
        {
            var documentStore = new DocumentStore { Url = ravenUrl }.Initialize();
            using (var session = documentStore.OpenSession(databaseName))
            {
                for (var i = 0; i < numberOfDocuments - 1; i++)
                {
                    session.Store(new Test()
                    {
                        Message = string.Format("Hi, I am message number {0}, for data import run at {1}", i, DateTime.Now.ToShortTimeString())
                    });
                }
                session.SaveChanges();
            }
        }

        private static void DeleteAllTestDatabaseDocuments(string httpLocalhost, string databaseName)
        {
            var documentStore = new DocumentStore { Url = httpLocalhost }.Initialize();

            while (documentStore.DatabaseCommands.ForDatabase(databaseName).GetStatistics().StaleIndexes.Length > 0)
                Thread.Sleep(1000);

            var operation = documentStore.DatabaseCommands.ForDatabase(databaseName).DeleteByIndex("Raven/DocumentsByEntityName",
                new IndexQuery
                {
                    Query = string.Format("Tag:{0}", Inflector.Pluralize(typeof(Test).Name))
                }, allowStale: false);

            operation.WaitForCompletion();
        }
    }

    public class Test
    {
        public string Message { get; set; }
    }
}
