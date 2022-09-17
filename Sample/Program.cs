namespace Sample
{

    internal class Program
    {

        static void Main(string[] args)
        {
            string azureConnectionString = "UseDevelopmentStorage=true;";

            var storage = new EasyAzStorage.AzureStorage(azureConnectionString);

            storage.Tables.CreateTableIfNotExist<Book>();

            var book1 = new Book()
            {
                PartitionKey = "0",
                RowKey = "0000001",
                Title = "Foundation and Empire",
                Author = "Isaac Asimov",
                NumOfPages = 247,
                PubDate = new DateTime(1952, 1, 1)
            };

            storage.Tables.Save(book1);

        }

    }

}