using System;
using System.Collections;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Runtime.Serialization;
using System.Security.Cryptography.Xml;
using System.Threading;
using System.Threading.Tasks;
using M220N.Models;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;

namespace Migrator
{

    class Program
    {
        static IMongoCollection<Movie> _moviesCollection;

        // TODO: Update this connection string as needed.
        static string mongoConnectionString = "mongodb://m001-student:m001-mongodb-basics@sandbox-shard-00-02-4vaz0.mongodb.net:27017/admin?authSource=admin&readPreference=secondary&appname=MongoDB%20Compass&ssl=true";
        
        static async Task Main(string[] args)
        {
            Setup();

            Console.WriteLine("Starting the data migration.");
            var datePipelineResults = TransformDatePipeline();
            Console.WriteLine($"I found {datePipelineResults.Count} docs where the lastupdated field is of type 'string'.");

            if (datePipelineResults.Count > 0)
            {
                /* ======================================================
                 I've tried all this lines commented and I have no luck
                 These links might help you:
                 - https://discourse.university.mongodb.com/t/chapter-4-admin-backend-ticket-migration/56016/2
                 - https://dev.to/mpetrinidev/a-guide-to-bulk-write-operations-in-mongodb-with-c-51fk                 
                  ====================================================== */

                //BulkWriteResult<Movie> bulkWriteDatesResult = null;
                //string datePattern = "yyyy-MM-dd HH:mm:ss";
                var listWrites = new List<WriteModel<Movie>>();
                // TODO Ticket: Call  _moviesCollection.BulkWriteAsync, passing in the
                // datePipelineResults. You will need to use a ReplaceOneModel<Movie>
                // (https://api.mongodb.com/csharp/current/html/T_MongoDB_Driver_ReplaceOneModel_1.htm).
                //
                //var filter = Builders<Movie>.Filter.In("countries", datePipelineResults);
                //var replaceModels = new List<ReplaceOneModel<Movie>>(datePipelineResults.Count);
                //bulkWriteDatesResult = await _moviesCollection.BulkWriteAsync(replaceModels, new BulkWriteOptions() { IsOrdered = false });

                for (int i = 0; i < datePipelineResults.Count; i++)
                {
                    listWrites.Add(new InsertOneModel<Movie>(datePipelineResults[i]));

                }

                //var filter = new FilterDefinitionBuilder<Movie>().Where(m => m.Id != null);
                //List<WriteModel<Movie>> requests = new List<WriteModel<Movie>>();

                //ReplaceOneModel<Movie> teste = new ReplaceOneModel<Movie>(filter, datePipelineResults[0]);
                //requests.Add(teste);

                //foreach (var item in datePipelineResults)
                //{
                //    ReplaceOneModel<Movie> teste = new ReplaceOneModel<Movie>(filter, item);

                //    requests.Add(teste);
                //}                

                var bulkWriteDatesResult = await _moviesCollection.BulkWriteAsync(listWrites);

                Console.WriteLine($"{bulkWriteDatesResult.ProcessedRequests.Count} records updated.");
                
            }

            var ratingPipelineResults = TransformRatingPipeline();
            Console.WriteLine($"I found {ratingPipelineResults.Count} docs where the imdb.rating field is not a number type.");

            if (ratingPipelineResults.Count > 0)
            {
                BulkWriteResult<Movie> bulkWriteRatingsResult = null;
                // TODO Ticket: Call  _moviesCollection.BulkWriteAsync, passing in the
                // ratingPipelineResults. You will need to use a ReplaceOneModel<Movie>
                // (https://api.mongodb.com/csharp/current/html/T_MongoDB_Driver_ReplaceOneModel_1.htm).
                //
                // // bulkWriteRatingsResult = await _moviesCollection.BulkWriteAsync(...

                Console.WriteLine($"{bulkWriteRatingsResult.ProcessedRequests.Count} records updated.");
            }

            Console.WriteLine();
            Console.WriteLine("Checking the data conversions...");
            Verify();

            // Keep the console window open until user hits `enter` or closes.
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("Press <Enter> to close.");
            Console.ReadLine();
        }

        static void Setup()
        {
            var camelCaseConvention = new ConventionPack {new CamelCaseElementNameConvention()};
            ConventionRegistry.Register("CamelCase", camelCaseConvention, type => true);

            var mongoUri = mongoConnectionString;
            var mflixClient = new MongoClient(mongoUri);            
            var moviesDatabase = mflixClient.GetDatabase("sample_mflix");
            _moviesCollection = moviesDatabase.GetCollection<Movie>("movies");
        }

        /// <summary>
        ///     Creates an aggregation pipeline that finds all documents 
        ///     that have a non-numeric 'imdb.rating' value 
        ///     and converts those values to type 'double'.
        /// 
        ///    The code below is the C# way to represent the following pipeline:
        ///    
        ///    [{'$match': {'imdb.rating': {'$not': {'$type': 'number'}}}}, {
        ///      '$addFields': {'imdb.rating': {'$convert': {
        ///          'input': 'imdb.rating', 'to': 'double', 'onError': -1}}}}]
        ///
        /// </summary>
        /// <returns>A List of Movie objects with the rating values converted.</returns>
        static List<Movie> TransformRatingPipeline()
        {
            var pipeline = new[]
            {
                new BsonDocument("$match",
                    new BsonDocument("imdb.rating",
                        new BsonDocument("$not",
                            new BsonDocument("$type", "number")))),
                new BsonDocument("$addFields",
                    new BsonDocument("imdb.rating",
                        new BsonDocument("$convert",
                            new BsonDocument
                            {
                                {"input", "imdb.rating"},
                                {"to", "double"},
                                {"onError", -1}
                            })))
            };

            return _moviesCollection
                .Aggregate(PipelineDefinition<Movie, Movie>.Create(pipeline))
                .ToList();
        }

        /// <summary>
        ///     Creates an aggregation pipeline that finds all documents 
        ///     that have a string 'lastupdated' value 
        ///     and converts those values to type 'date'.
        /// 
        ///    The code below is the C# way to represent the following pipeline:
        ///    
        ///     [{'$match': {'lastupdated': {'$type': 2}}}, {
        ///          '$addFields': {'lastupdated': {'$dateFromString': {
        ///          'dateString': {'$substr': ['$lastupdated', 0, 23]}}}}}]
        ///
        /// </summary>
        /// <returns>A List of Movie objects with the lastupdated values converted to dates.</returns>
        static List<Movie> TransformDatePipeline()
        {
            var pipeline = new[]
            {
                new BsonDocument("$match",
                    new BsonDocument("lastupdated",
                        new BsonDocument("$type", 2))),
                new BsonDocument("$addFields",
                    new BsonDocument("lastupdated",
                        new BsonDocument("$dateFromString",
                            new BsonDocument
                            {
                                {
                                    "dateString",
                                    new BsonDocument("$substr",
                                        new BsonArray
                                        {
                                            "$lastupdated",
                                            0,
                                            23
                                        })
                                },
                                {"timezone", "America/New_York"}
                            })))
            };

            return _moviesCollection
                .Aggregate(PipelineDefinition<Movie, Movie>.Create(pipeline))
                .ToList();
        }

        static void Verify()
        {
            var pipeline = new[]
            {
                new BsonDocument("$match",
                    new BsonDocument("$or",
                        new BsonArray
                        {
                            new BsonDocument("lastupdated",
                                new BsonDocument("$type", "string")),
                            new BsonDocument("imdb.rating",
                                new BsonDocument("$type", "string"))
                        })),
                new BsonDocument("$count", "badDocs")
            };

            var badDocs = _moviesCollection
                .Aggregate(PipelineDefinition<Movie, BsonDocument>.Create(pipeline))
                .ToList();

            if (badDocs.Count == 0)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("[✓] No remaining docs to be converted. Great job!");
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"[ ] Uh oh. One or both of your pipelines missed {badDocs.Count} documents...");
            }
        }
    }
}
