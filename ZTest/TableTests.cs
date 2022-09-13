using Azure.Data.Tables;
using Microsoft.Extensions.Configuration;

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

            // TODO : AddUserSecrets . . .


            // "the type specified here is just so the secrets library can 
            // find the UserSecretId we added in the csproj file"
            // Ref: https://patrickhuber.github.io/2017/07/26/avoid-secrets-in-dot-net-core-tests.html

            var builder = new ConfigurationBuilder()
                .AddUserSecrets<TableTests>();

            var Configuration = builder.Build();
            string k1 = Configuration["key1"];
            string k2 = Configuration["key2"];

            _azureConnectionString = Configuration["AzureStorageAccountConnString"];
            if (_azureConnectionString == null)
                throw new ApplicationException("AzureStorageAccountConnString is null");


            // TODO: clear table or delete/create table
            var storage = new easyazstorage.AzureStorage(_azureConnectionString);
            storage.Tables.CreateTableIfNotExist<Person>();


        }



        [Test]
        public void Test1()
        {
            return; // -----------------

            var storage = new easyazstorage.AzureStorage(_azureConnectionString);

            storage.Tables.CreateTableIfNotExist<Person>();

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
        public void Test2()
        {
            return; // -----------------

            var storage = new easyazstorage.AzureStorage(_azureConnectionString);


            for (int i = 0; i < 2000; i++)
            {
                var p = new Person() { PartitionKey = "B", RowKey = i.ToString(), FirstName = Guid.NewGuid().ToString(), LastName = "guid", BirthDate = DateTime.UtcNow };
                storage.Tables.Save(p);
            }
        }


        [Test]
        public void TopN()
        {
            var storage = new easyazstorage.AzureStorage(_azureConnectionString);

            string pk = "TopN";

            List<Person> people;

            people = storage.Tables.RunQuery<Person>(p => p.PartitionKey == pk);

            //if (people.Count != 2000)
            //{
            //    for (int i = 0; i < 2000; i++)
            //    {
            //        var p = new Person() { PartitionKey = pk, RowKey = i.ToString(), FirstName = Guid.NewGuid().ToString(), LastName = "guid", BirthDate = DateTime.UtcNow };
            //        storage.Tables.Save(p);
            //    }
            //}





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
        public void First()
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
        public void DeleteSingle()
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
        public void SaveBatch()
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
            storage.Tables.SaveBatch(people.Take(1).ToList());

            // 100 items (max)
            storage.Tables.SaveBatch(people.Take(100).ToList());

            // too many items
            var ex1 = Assert.Throws<TableTransactionFailedException>(() => { storage.Tables.SaveBatch(people.Take(101).ToList()); });
            Assert.IsTrue(ex1.Status == 400);
            Assert.IsTrue(ex1.FailedTransactionActionIndex == 99);

            // items not all on the same partion-key
            people[5].PartitionKey = "break";
            var ex2 = Assert.Throws<TableTransactionFailedException>(() => { storage.Tables.SaveBatch(people.Take(10).ToList()); });
            Assert.IsTrue(ex2.Status == 400);
            Assert.IsTrue(ex2.FailedTransactionActionIndex == 5);
        }



        [Test]
        public void SaveAutoBatch()
        {
            string pk = "AutoBatch";

            List<Person> people = new List<Person>(); ;
            for (int i = 0; i < 1000; i++)
            {
                var p = new Person() { PartitionKey = pk + "_" + (i % 7).ToString(), RowKey = i.ToString(), FirstName = Guid.NewGuid().ToString(), LastName = "guid", BirthDate = DateTime.UtcNow };
                people.Add(p);
            }

            var storage = new easyazstorage.AzureStorage(_azureConnectionString);
            int n = storage.Tables.SaveAutoBatch(people);

            Assert.IsTrue(n == 14);

            var items = storage.Tables.RunQuery<Person>(p => p.PartitionKey.CompareTo("AutoBatch_0") >= 0
                                                          && p.PartitionKey.CompareTo("AutoBatch_9") < 0);

            Assert.IsTrue(items.Count == 1000);

        }




        [Test]
        public void DeleteBatch()
        {
            Assert.Fail();
        }



        [Test]
        public void InsertBatch()
        {
            Assert.Fail();
        }


    }
}