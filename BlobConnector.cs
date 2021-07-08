using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Hadoop.Avro.Container;

namespace ExtractAvroContentsFunction
{
    internal class BlobConnector
    {
        internal static CloudBlobContainer container = null;
        internal static CloudBlockBlob getBlobFileRef(
            string connectionString,
            string containerName,
            string fileName
            )
        {
            if (container == null)
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);

                // Connect to the blob storage
                CloudBlobClient serviceClient = storageAccount.CreateCloudBlobClient();

                // Connect to the blob container
                container = serviceClient.GetContainerReference(containerName);
            }
                
            // Connect to the blob file
            CloudBlockBlob blob = container.GetBlockBlobReference(fileName);

            return blob;
        }
    }
}
