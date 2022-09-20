using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ZTest
{
    public class BlobTests
    {

        private string _azureConnectionString = null;
        private const string _containerName = "test001";
        private Random random = new Random();


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

            // TODO: clear container or create a new one
            var storage = new EasyAzStorage.AzureStorage(_azureConnectionString);
            storage.Blobs.CreateContainerIfNotExist(_containerName);
        }


        [Test]
        public void ReadWriteExistsDelete()
        {
            var storage = new EasyAzStorage.AzureStorage(_azureConnectionString);

            byte[] data1 = GetRandomBytes(100);
            byte[] data2 = GetRandomBytes(1 * 1024);
            byte[] data3 = GetRandomBytes(1 * 1024 * 1024);


            storage.Blobs.StoreData(_containerName, "data1.dat", data1);
            storage.Blobs.StoreData(_containerName, "data2.dat", data2);
            storage.Blobs.StoreData(_containerName, "data3.dat", data3);


            byte[] data1_out = storage.Blobs.GetData(_containerName, "data1.dat");
            byte[] data2_out = storage.Blobs.GetData(_containerName, "data2.dat");
            byte[] data3_out = storage.Blobs.GetData(_containerName, "data3.dat");


            Assert.IsTrue(data1.SequenceEqual(data1_out));
            Assert.IsTrue(data2.SequenceEqual(data2_out));
            Assert.IsTrue(data3.SequenceEqual(data3_out));

            Assert.Throws<Azure.RequestFailedException>(() => storage.Blobs.GetData(_containerName, "non_existing.dat"));

            Assert.IsTrue(storage.Blobs.BlobExists(_containerName, "data1.dat"));
            Assert.IsFalse(storage.Blobs.BlobExists(_containerName, "non_existing.dat"));


            storage.Blobs.Delete(_containerName, "data3.dat");
            Assert.IsFalse(storage.Blobs.BlobExists(_containerName, "data3.dat"));
        }


        [Test]
        public void Listing()
        {

            var storage = new EasyAzStorage.AzureStorage(_azureConnectionString);


            //storage.Blobs.StoreData(_containerName, "f1.dat", new byte[] { 100, 101, 202 });
            //storage.Blobs.StoreData(_containerName, "a/f1.dat", new byte[] { 100, 101, 202 });
            //storage.Blobs.StoreData(_containerName, "a\\f2.dat", new byte[] { 100, 101, 202 });
            //storage.Blobs.StoreData(_containerName, "b/f1.dat", new byte[] { 100, 101, 202 });
            //storage.Blobs.StoreData(_containerName, "b\\f2.dat", new byte[] { 100, 101, 202 });

            //storage.Blobs.StoreData(_containerName, "c/f1.dat", new byte[] { 100, 101, 202 });
            //storage.Blobs.StoreData(_containerName, "c/1/2/3/4/f1.dat", new byte[] { 100, 101, 202 });


            var blobList = storage.Blobs.List2(_containerName);

            foreach (var b in blobList)
            {
                Console.WriteLine(b.Name);
            }

        }


        [Test]
        public void StoreDataParallel()
        {
            // On my network:
            //  1000 x  1K -->  4.5 s.
            //  1000 x 10K -->  5.0 s.
            // 10000 x  1K --> 38.0 s.
            // 10000 x 10K --> 45.0 s.

            var storage = new EasyAzStorage.AzureStorage(_azureConnectionString);

            var blobList = Enumerable.Range(0, 1000).Select(i => ("parallel1/blob_" + i, GetRandomBytes(1 * 1024))).ToList();

            //storage.Blobs.StoreDataParallel(_containerName, blobList);

            var blobNameList = blobList.Select(x => x.Item1).ToList();


            var blobListOUT = storage.Blobs.GetDataParallel(_containerName, blobNameList);



        }









        private byte[] GetRandomBytes(int len)
        {
            var rnd = new Random();
            var buffer = new byte[len];
            rnd.NextBytes(buffer);
            return buffer;
        }


    }
}
