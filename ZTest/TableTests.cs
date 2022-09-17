using Azure.Data.Tables;
using Microsoft.Extensions.Configuration;
using System.Diagnostics;

namespace ZTest
{
    public class TableTests
    {

        private string _azureConnectionString = null; //"UseDevelopmentStorage=true;";


        [SetUp]
        public void Setup()
        {

        }


        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            // Configuration:
            // Note: the type specified here is just so the secrets library can 
            //       find the UserSecretId we added in the csproj file"
            //       Ref: https://patrickhuber.github.io/2017/07/26/avoid-secrets-in-dot-net-core-tests.html

            var builder = new ConfigurationBuilder()
                                 .AddUserSecrets<TableTests>();

            var Configuration = builder.Build();
            _azureConnectionString = Configuration["AzureStorageAccountConnString"];
            if (_azureConnectionString == null)
                throw new ApplicationException("AzureStorageAccountConnString is null");

            // TODO: clear table or delete/create table
            var storage = new easyazstorage.AzureStorage(_azureConnectionString);
            storage.Tables.CreateTableIfNotExist<Person>();
        }



        [Test]
        public void Retrieve()
        {
            var storage = new easyazstorage.AzureStorage(_azureConnectionString);

            var p1 = new Person() { PartitionKey = "A", RowKey = "0", FirstName = "Fabrizio", LastName = "ABC", BirthDate = DateTime.UtcNow };
            var p2 = new Person() { PartitionKey = "A", RowKey = "1", FirstName = "Sabry", LastName = "ABC", BirthDate = DateTime.UtcNow };

            storage.Tables.Save(p1);
            storage.Tables.Save(p2);

            Assert.IsNull(storage.Tables.Retrieve<Person>("nodata", "nodata"));

            var p1_retrieve = storage.Tables.Retrieve<Person>("A", "0");
            Assert.IsNotNull(p1_retrieve);
            Assert.That(p1_retrieve.BirthDate, Is.EqualTo(p1.BirthDate));
        }


        [Test]
        public void RetrieveParallel()
        {
            var stopwatch = new Stopwatch();

            var storage = new easyazstorage.AzureStorage(_azureConnectionString);

            string pk_root = "RetPar_";
            int numOfItems = 100;

            // Fill data
            List<Person> peopleIN = new List<Person>();
            for (int i = 0; i < numOfItems; i++)
            {
                var p = new Person() { PartitionKey = pk_root + (i % 10), RowKey = i.ToString(), FirstName = Guid.NewGuid().ToString(), LastName = "guid", BirthDate = DateTime.UtcNow };
                peopleIN.Add(p);
            }
            storage.Tables.SaveMultiBatch(peopleIN);

            // parallel retrieve
            stopwatch.Restart();
            var filter = peopleIN.Select(x => (x.PartitionKey, x.RowKey)).ToArray();
            List<Person> peopleOUTPar = storage.Tables.RetrieveParallel<Person>(filter);
            Console.WriteLine($"items count: {peopleOUTPar.Count}  timer: {stopwatch.Elapsed}");

            // sequencial retrieve (classical)
            stopwatch.Restart();
            List<Person> peopleOUTSeq = new List<Person>();
            peopleIN.ForEach(p => peopleOUTSeq.Add(storage.Tables.Retrieve<Person>(p.PartitionKey, p.RowKey)));
            Console.WriteLine($"items count: {peopleOUTSeq.Count}  timer: {stopwatch.Elapsed}");

            // checks
            var peopleINOrdered = peopleIN.OrderBy(p => p.PartitionKey).ThenBy(p => p.RowKey).ToList();
            var peopleOUTParOrdered = peopleOUTPar.OrderBy(p => p.PartitionKey).ThenBy(p => p.RowKey).ToList();
            var peopleOUTSeqOrdered = peopleOUTSeq.OrderBy(p => p.PartitionKey).ThenBy(p => p.RowKey).ToList();
            Assert.IsTrue(ComparePeople(peopleINOrdered, peopleOUTParOrdered));
            Assert.IsTrue(ComparePeople(peopleINOrdered, peopleOUTSeqOrdered));
        }


        private bool ComparePeople(List<Person> people1, List<Person> people2)
        {
            if (people1.Count != people2.Count)
                return false;

            for (int i = 0; i < people1.Count; i++)
            {
                if (!ComparePerson(people1[i], people2[i]))
                    return false;
            }

            return true;
        }

        private bool ComparePerson(Person p1, Person p2)
        {
            // Note : I could implement IComparable<Person> but I do not want to  dirty Person class.

            return
                (p1.PartitionKey == p2.PartitionKey) &&
                (p1.RowKey == p2.RowKey) &&
                (p1.FirstName == p2.FirstName) &&
                (p1.LastName == p1.LastName) &&
                (p1.BirthDate == p2.BirthDate);
        }



        [Test]
        public void Save()
        {
            var storage = new easyazstorage.AzureStorage(_azureConnectionString);

            var p_OK = new Person() { PartitionKey = "Single", RowKey = "00000", FirstName = Guid.NewGuid().ToString(), LastName = "guid", BirthDate = DateTime.UtcNow };
            var p_ERR = new Person() { PartitionKey = "####", RowKey = "#####", FirstName = Guid.NewGuid().ToString(), LastName = "guid", BirthDate = DateTime.UtcNow };

            storage.Tables.Save(p_OK);

            Assert.Throws<Azure.RequestFailedException>(() => storage.Tables.Save(p_ERR));
        }


        [Test]
        public void TopN()
        {
            var storage = new easyazstorage.AzureStorage(_azureConnectionString);

            string pk = "TopN";

            // Build test data
            var peopleData = new List<Person>();
            for (int i = 0; i < 2000; i++)
            {
                peopleData.Add(new Person() { PartitionKey = pk, RowKey = i.ToString(), FirstName = Guid.NewGuid().ToString(), LastName = "guid", BirthDate = DateTime.UtcNow });
            }
            int n = storage.Tables.SaveMultiBatch(peopleData);
            Console.WriteLine(n);


            // tests
            List<Person> people;

            people = storage.Tables.RunQuery<Person>(p => p.PartitionKey == pk);
            Assert.That(people.Count, Is.EqualTo(2000));

            people = storage.Tables.RunQuery<Person>(p => p.PartitionKey == pk, 50);
            Assert.That(people.Count, Is.EqualTo(50));

            people = storage.Tables.RunQuery<Person>(p => p.PartitionKey == pk, 1500);
            Assert.That(people.Count, Is.EqualTo(1500));

            people = storage.Tables.RunQuery<Person>(p => p.PartitionKey == pk, 1);
            Assert.That(people.Count, Is.EqualTo(1));

            people = storage.Tables.RunQuery<Person>(p => p.PartitionKey == "DOESNOTEXIST", 1);
            Assert.That(people.Count, Is.EqualTo(0));
        }



        [Test]
        public void FirstOfTable()
        {
            var storage = new easyazstorage.AzureStorage(_azureConnectionString);

            string zeros = "000000";

            var p1 = new Person() { PartitionKey = zeros, RowKey = zeros, FirstName = zeros, LastName = zeros, BirthDate = DateTime.UtcNow };
            storage.Tables.Save(p1);

            var p2 = storage.Tables.First<Person>();

            Assert.IsNotNull(p2);
            Assert.That(p2.PartitionKey, Is.EqualTo(zeros));
            Assert.That(p2.RowKey, Is.EqualTo(zeros));
        }


        [Test]
        public void GetAll()
        {
            var storage = new easyazstorage.AzureStorage(_azureConnectionString);

            var people = storage.Tables.RunQuery<Person>(null);

            Console.WriteLine(people.Count);

            Assert.IsTrue(people.Count > 0);
        }


        [Test]
        public void Delete()
        {
            var storage = new easyazstorage.AzureStorage(_azureConnectionString);

            string id = "tobedeleted";

            var p = new Person() { PartitionKey = id, RowKey = id, BirthDate = DateTime.UtcNow };

            storage.Tables.Save(p);

            // silent delete of non existing item
            storage.Tables.Delete(new Person() { PartitionKey = "xyz", RowKey = "xyz" });

            // non-silent delete of non existing item
            Assert.Throws(
                typeof(ApplicationException),
                () =>
                {
                    storage.Tables.Delete(new Person() { PartitionKey = "xyz", RowKey = "xyz" }, throwIfNotFound: true);
                });

            // delete existing item
            storage.Tables.Delete(p, true);
        }




        [Test]
        public void SaveBacthTransaction()
        {
            // Warning: Azurite bug : does not correctly manage batches
            // https://github.com/Azure/Azurite/issues/1215
            // So, run this test against a real Azure Storage Account

            string pk = "Batch";

            List<Person> people = new List<Person>(); ;
            for (int i = 0; i < 200; i++)
            {
                var p = new Person() { PartitionKey = pk, RowKey = i.ToString(), FirstName = Guid.NewGuid().ToString(), LastName = "guid", BirthDate = DateTime.UtcNow };
                people.Add(p);
            }

            var storage = new easyazstorage.AzureStorage(_azureConnectionString);

            // 1 item
            storage.Tables.SaveBacthTransaction(people.Take(1).ToList());

            // 100 items (max)
            storage.Tables.SaveBacthTransaction(people.Take(100).ToList());

            // too many items
            var ex1 = Assert.Throws<TableTransactionFailedException>(() => { storage.Tables.SaveBacthTransaction(people.Take(101).ToList()); });
            Assert.IsTrue(ex1.Status == 400);
            Assert.IsTrue(ex1.FailedTransactionActionIndex == 99);

            // items not all on the same partion-key --> transaction rollback
            people[115].PartitionKey = "break";
            var ex2 = Assert.Throws<TableTransactionFailedException>(() => { storage.Tables.SaveBacthTransaction(people.Skip(100).Take(40).ToList()); });
            Assert.IsTrue(ex2.Status == 400);
            Assert.IsTrue(ex2.FailedTransactionActionIndex == 15);
            Assert.IsNull(storage.Tables.Retrieve<Person>(pk, "114"));
            Assert.IsNull(storage.Tables.Retrieve<Person>(pk, "116"));
        }



        [Test]
        public void SaveMultiBatch()
        {
            string pk = "AutoBatch";

            List<Person> people = new List<Person>(); ;
            for (int i = 0; i < 1000; i++)
            {
                var p = new Person() { PartitionKey = pk + "_" + (i % 7).ToString(), RowKey = i.ToString(), FirstName = Guid.NewGuid().ToString(), LastName = "guid", BirthDate = DateTime.UtcNow };
                people.Add(p);
            }

            var storage = new easyazstorage.AzureStorage(_azureConnectionString);
            int n = storage.Tables.SaveMultiBatch(people);

            Assert.IsTrue(n == 14);
            var items = storage.Tables.RunQuery<Person>(p => p.PartitionKey.CompareTo("AutoBatch_0") >= 0
                                                          && p.PartitionKey.CompareTo("AutoBatch_9") < 0);
            Assert.IsTrue(items.Count == 1000);

            // send a not valid item
            people[17].PartitionKey = "###";  // '#' is not a valid character for partion keys
            Assert.Catch<TableTransactionFailedException>(() => { storage.Tables.SaveMultiBatch(people); });
        }






        [Test]
        public void DeleteBatchTransaction()
        {
            var storage = new easyazstorage.AzureStorage(_azureConnectionString);

            string pk = "delete";

            // prepare test data
            List<Person> people = new List<Person>(); ;
            for (int i = 0; i < 200; i++)
            {
                var p = new Person() { PartitionKey = pk, RowKey = i.ToString(), FirstName = Guid.NewGuid().ToString(), LastName = "guid", BirthDate = DateTime.UtcNow };
                people.Add(p);
            }
            storage.Tables.SaveMultiBatch(people);


            // too many items
            Assert.Throws<TableTransactionFailedException>(() => storage.Tables.DeleteBatchTransaction(people));

            // ok
            storage.Tables.DeleteBatchTransaction(people.Take(1).ToList());
            storage.Tables.DeleteBatchTransaction(people.Skip(1).Take(10).ToList());
            storage.Tables.DeleteBatchTransaction(people.Skip(11).Take(100).ToList());

            // delete already deleted item
            storage.Tables.DeleteBatchTransaction(people.Take(1).ToList());

            Azure.Data.Tables.TableTransactionFailedException


            Assert.Fail("TODO");
        }


        public void DeleteMultiBatch()
        {
            var storage = new easyazstorage.AzureStorage(_azureConnectionString);


            Assert.Fail("TODO");
        }


    }
}