namespace ZTest
{
    public class TableTests
    {

        private const string AzureConnectionString = "UseDevelopmentStorage=true;";


        [SetUp]
        public void Setup()
        {
        }


        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            // TODO: clear table or delete/create table
            var storage = new easyazstorage.AzureStorage(AzureConnectionString);
            storage.Tables.CreateTableIfNotExist<Person>();
        }



        [Test]
        public void Test1()
        {
            var storage = new easyazstorage.AzureStorage(AzureConnectionString);

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
            var storage = new easyazstorage.AzureStorage(AzureConnectionString);


            for (int i = 0; i < 2000; i++)
            {
                var p = new Person() { PartitionKey = "B", RowKey = i.ToString(), FirstName = Guid.NewGuid().ToString(), LastName = "guid", BirthDate = DateTime.UtcNow };
                storage.Tables.Save(p);
            }
        }


        [Test]
        public void TopN()
        {
            var storage = new easyazstorage.AzureStorage(AzureConnectionString);

            string pk = "TopN";

            List<Person> people;

            people = storage.Tables.RunQuery<Person>(p => p.PartitionKey == pk);

            if (people.Count != 2000)
            {
                for (int i = 0; i < 2000; i++)
                {
                    var p = new Person() { PartitionKey = pk, RowKey = i.ToString(), FirstName = Guid.NewGuid().ToString(), LastName = "guid", BirthDate = DateTime.UtcNow };
                    storage.Tables.Save(p);
                }
            }

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
            var storage = new easyazstorage.AzureStorage(AzureConnectionString);

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
            var storage = new easyazstorage.AzureStorage(AzureConnectionString);

            var people = storage.Tables.RunQuery<Person>(null);

            Console.WriteLine(people.Count);

            Assert.IsTrue(people.Count > 0);
        }


        [Test]
        public void DeleteSingle()
        {
            var storage = new easyazstorage.AzureStorage(AzureConnectionString);

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
            // AZURITE bug : does not correctly manage batches
            // max 100 items

            string pk = "Batch";

            List<Person> people = new List<Person>(); ;




            for (int i = 0; i < 100; i++)
            {
                var p = new Person() { PartitionKey = pk+i.ToString(), RowKey = i.ToString(), FirstName = Guid.NewGuid().ToString(), LastName = "guid", BirthDate = DateTime.UtcNow };
                people.Add(p);
            }

            var storage = new easyazstorage.AzureStorage(AzureConnectionString);
            storage.Tables.SaveBatch(people);

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