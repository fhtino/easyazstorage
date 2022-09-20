﻿using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace EasyAzStorage
{



    public class EasyBlob
    {

        public class BlobInfo
        {
            public string Name { get; set; }
            public DateTime? LastModified { get; set; }
        }



        // info : https://github.com/Azure/azure-sdk-for-net/blob/main/sdk/storage/Azure.Storage.Blobs/README.md

        private string _connectionString;


        public EasyBlob(string connString)
        {
            _connectionString = connString;
        }


        public void CreateContainerIfNotExist(string containerName)
        {
            this.GetAzureBlobClient(containerName, true);
        }


        public bool BlobExists(string containerName, string blobName)
        {
            var client = GetAzureBlobClient(containerName);
            var blob = client.GetBlobClient(blobName);
            return blob.Exists();
        }


        public void StoreData(string containerName, string blobName, byte[] data, bool overwrite = true)
        {
            var client = GetAzureBlobClient(containerName);
            var blob = client.GetBlobClient(blobName);
            blob.Upload(new BinaryData(data), overwrite: overwrite);
        }


        public byte[] GetData(string containerName, string blobName)
        {
            var client = GetAzureBlobClient(containerName);
            var blob = client.GetBlobClient(blobName);
            var ms = new MemoryStream();
            blob.DownloadTo(ms);
            return ms.ToArray();
        }


        public void Delete(string containerName, string blobName)
        {
            var client = GetAzureBlobClient(containerName);
            var blob = client.GetBlobClient(blobName);
            blob.Delete();
        }


        public void StoreDataParallel(string containerName, List<(string blobName, byte[] data)> items, bool overwrite = true)
        {
            // There is BUG in the library / .net 6  ????
            // https://github.com/Azure/azure-sdk-for-net/issues/27018
            // Currently I'm not able to reproduce it.

            var client = GetAzureBlobClient(containerName);

            Parallel.ForEach(
                items, 
                //new ParallelOptions() { MaxDegreeOfParallelism = 1 },
                item =>
                {
                    var blob = client.GetBlobClient(item.blobName);
                    blob.Upload(new BinaryData(item.data), overwrite: overwrite);
                });

            // throw new NotImplementedException("not yet impleted - bug?");
        }


        public List<(string blobName, byte[] data)> GetDataParallel(string containerName, List<string> blobNameList)
        {
            var client = GetAzureBlobClient(containerName);

            var outputList = new ConcurrentBag<(string blobName, byte[] data)>();


            //System.Collections.Concurrent.ConcurrentBag

            Parallel.ForEach(
               blobNameList,
               //new ParallelOptions() { MaxDegreeOfParallelism = 1 },
               blobName =>
               {
                   var blob = client.GetBlobClient(blobName);
                   var ms = new MemoryStream();
                   blob.DownloadTo(ms);
                   outputList.Add((blobName, ms.ToArray()));
               });

            return outputList.ToList();
        }


        public List<BlobInfo> List(string containerName, string prefix = null)
        {
            var client = GetAzureBlobClient(containerName);
            var outList = new List<BlobInfo>();
            foreach (var item in client.GetBlobs(prefix: prefix))
            {
                outList.Add(
                    new BlobInfo()
                    {
                        Name = item.Name,
                        LastModified = item.Properties.LastModified?.UtcDateTime
                    });
            }
            return outList;
        }



        public async Task<List<BlobInfo>> ListAsync(string containerName, string prefix = null)
        {
            throw new NotImplementedException("Work in progress");

            var client = GetAzureBlobClient(containerName);
            var outList = new List<BlobInfo>();


            Azure.AsyncPageable<BlobItem> response = client.GetBlobsAsync(prefix: prefix);

            // https://devblogs.microsoft.com/dotnet/configureawait-faq/
            // https://learn.microsoft.com/en-us/archive/msdn-magazine/2019/november/csharp-iterating-with-async-enumerables-in-csharp-8

            // response.ConfigureAwait(false) ;  // ???

            var enumerator = response.GetAsyncEnumerator();

            while (await enumerator.MoveNextAsync())
            {
                BlobItem item = enumerator.Current;
                outList.Add(
                   new BlobInfo()
                   {
                       Name = item.Name,
                       LastModified = item.Properties.LastModified?.UtcDateTime
                   });
            }

            return outList;
        }



        public List<BlobInfo> List2(string containerName, string prefix = null)
        {
            //  info:  https://github.com/Azure/azure-sdk-for-net/issues/12315

            var client = GetAzureBlobClient(containerName);

            prefix = null;

            string delimiter = "/";

            var outList = new List<BlobInfo>();

            // if you specify the delimiter, the listing:
            //   - is based on prefix
            //   - is NOT recursive.
            //   - contains 'directories' and 'files'


            foreach (var item in client.GetBlobsByHierarchy(prefix: prefix, delimiter: delimiter))
            {
                Console.WriteLine(item.Prefix + ":IsPrefix=" + item.IsPrefix + ":IsBlob=" + item.IsBlob + ":" + (item.IsBlob ? item.Blob.Name : "-"));

                //item.
                //outList.Add(
                //    new BlobInfo()
                //    {
                //        Name = item.Name,
                //        LastModified = item.Properties.LastModified?.UtcDateTime
                //    });
            }



            //client.GetBlobsByHierarchy

            return outList;
        }






        private BlobContainerClient GetAzureBlobClient(string containerName, bool createIfNotExist = false)
        {
            var client = new BlobContainerClient(_connectionString, containerName);
            if (createIfNotExist)
                client.CreateIfNotExists();
            return client;
        }


    }

}