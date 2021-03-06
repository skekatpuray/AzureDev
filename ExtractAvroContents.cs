using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Hadoop.Avro.Container;
using System.Text;


namespace ExtractAvroContentsFunction
{
    
    public class Payload
    {
        public string Container { get; set; }

        public string SourceLocation { get; set; }

        public string DestinationLocation { get; set; }

        public string FileName { get; set; }
    }

    public static class ExtractAvroContents
    {
        [FunctionName("ExtractAvroContents")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string filename = req.Query["filename"];

            string avroFileName = String.Empty;
            string payloadJsonString = String.Empty;

            Payload payload = new Payload();

            using (StreamReader strmReader = new StreamReader(req.Body))
            {
                payloadJsonString = await strmReader.ReadToEndAsync();
                payload = JsonConvert.DeserializeObject<Payload>(payloadJsonString);
            }

            filename = avroFileName;
            try
            {
                string connectionString = Environment.GetEnvironmentVariable("DATALAKE_CONNECTIONSTRING", EnvironmentVariableTarget.Process);

                string containerName = Environment.GetEnvironmentVariable("CONTAINER_NAME", EnvironmentVariableTarget.Process);

                //Make this configurable
                string fileName = "raw/input/" + filename;

                //Make this configurable
                string outputFile = "raw/output/" + filename.Substring(0, filename.IndexOf('.')) + ".json";

                /*
                 * Create input file reference
                 */
                var inputFileRef = BlobConnector.getBlobFileRef(
                    Environment.GetEnvironmentVariable("DatalakeConnectionString", EnvironmentVariableTarget.Process),
                    containerName,
                    fileName);                


                /*
                 * Stream in file contents 
                 */
                var inputFileRefStream = new MemoryStream();

                await inputFileRef.DownloadToStreamAsync(inputFileRefStream);

                inputFileRefStream.Seek(0, SeekOrigin.Begin);

                StringBuilder strBuilder = new StringBuilder();                

                /*
                 * Iterate thru avro records, read the intended attribute (Body) and write it to StringBuilder.
                 */ 
                using (var reader = AvroContainer.CreateGenericReader(inputFileRefStream))
                {
                    while (reader.MoveNext())
                    {
                        foreach (dynamic record in reader.Current.Objects)
                        {
                            var sequenceNumber = record.SequenceNumber;
                            var bodyText = System.Text.Encoding.UTF8.GetString(record.Body);
                            strBuilder.Append(bodyText);
                        }
                    }
                }

                /*
                 * Create output file reference
                 */
                var outputFileRef = BlobConnector.getBlobFileRef(
                    Environment.GetEnvironmentVariable("DatalakeConnectionString", EnvironmentVariableTarget.Process),
                    containerName,
                    outputFile);

                /*
                 * Sink the output
                 */ 
                await outputFileRef.UploadTextAsync(strBuilder.ToString());

                return new OkObjectResult("File created successfully");
            }
            catch(Exception exp)
            {                
                return new BadRequestObjectResult("Error:  " + exp.Message + " " + exp.StackTrace);
            }            
        }
    }
}
