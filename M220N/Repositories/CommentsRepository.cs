using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using M220N.Models;
using M220N.Models.Projections;
using M220N.Models.Responses;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Driver;

namespace M220N.Repositories
{
    public class CommentsRepository
    {
        private readonly IMongoCollection<Comment> _commentsCollection;
        private readonly MoviesRepository _moviesRepository;

        public CommentsRepository(IMongoClient mongoClient)
        {
            var camelCaseConvention = new ConventionPack {new CamelCaseElementNameConvention()};
            ConventionRegistry.Register("CamelCase", camelCaseConvention, type => true);

            _commentsCollection = mongoClient.GetDatabase("sample_mflix").GetCollection<Comment>("comments");
            _moviesRepository = new MoviesRepository(mongoClient);
        }

        /// <summary>
        ///     Adds a comment.
        /// </summary>
        /// <param name="user"></param>
        /// <param name="movieId"></param>
        /// <param name="comment"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>The Movie associated with the comment.</returns>
        public async Task<Movie> AddCommentAsync(User user, ObjectId movieId, string comment,
            CancellationToken cancellationToken = default)
        {
            try
            {
                var newComment = new Comment
                {
                    Date = DateTime.UtcNow,
                    Text = comment,
                    Name = user.Name,
                    Email = user.Email,
                    MovieId = movieId
                };

                // Ticket: Add a new Comment
                // Implement InsertOneAsync() to insert a
                // new comment into the comments collection.
                await _commentsCollection.InsertOneAsync(newComment);


                return await _moviesRepository.GetMovieAsync(movieId.ToString(), cancellationToken);
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        ///     Updates an existing comment. Only the comment owner can update the comment.
        /// </summary>
        /// <param name="user"></param>
        /// <param name="movieId"></param>
        /// <param name="commentId"></param>
        /// <param name="comment"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>An UpdateResult</returns>
        public async Task<UpdateResult> UpdateCommentAsync(User user,
            ObjectId movieId, ObjectId commentId, string comment,
            CancellationToken cancellationToken = default)
        {
            // Ticket: Update a Comment
            // Implement UpdateOneAsync() to update an
            // existing comment. Remember that only the original
            // comment owner can update the comment!
            //
             return await _commentsCollection.UpdateOneAsync(
             Builders<Comment>.Filter.Where(c => c.MovieId == movieId && c.Email == user.Email),             
             Builders<Comment>.Update.Set(c => c.Text, comment),//.Set(...),
             new UpdateOptions(),
             cancellationToken);


        }

        /// <summary>
        ///     Deletes a comment. Only the comment owner can delete a comment.
        /// </summary>
        /// <param name="movieId"></param>
        /// <param name="commentId"></param>
        /// <param name="user"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>The movie associated with the comment that is being deleted.</returns>
        public async Task<Movie> DeleteCommentAsync(ObjectId movieId, ObjectId commentId,
            User user, CancellationToken cancellationToken = default)
        {
            // Ticket: Delete a Comment
            // Implement DeleteOne() to delete an
            // existing comment. Remember that only the original
            // comment owner can delete the comment!
            _commentsCollection.DeleteOne(
                Builders<Comment>.Filter.Where(
                    c => c.MovieId == movieId
                         && c.Id == commentId && c.Email == user.Email));

            return await _moviesRepository.GetMovieAsync(movieId.ToString(), cancellationToken);
        }

        public async Task<TopCommentsProjection> MostActiveCommentersAsync()
        {
            /**
                TODO Ticket: User Report
                Build a pipeline that returns the 20 most frequent commenters on the MFlix
                site. You can do this by counting the number of occurrences of a user's
                email in the `comments` collection.

                In addition, set the ReadConcern on the _commentsCollection to
                ensure the most accurate reads occur.
            */
            try
            {
                List<ReportProjection> result = new List<ReportProjection>();
                // TODO Ticket: User Report
                // Return the 20 users who have commented the most on MFlix. You will need to use
                // the Group, Sort, Limit, and Project methods of the Aggregation pipeline.
                
                
                var group = new BsonDocument { { "$sortByCount", "$email" } };
                var limit = new BsonDocument("$limit", 20); ;

                var pipeline = new[]
                {
                    group,
                    limit
                };
                
                var project = Builders<Comment>.Projection.Include(m => m.Name).Include(m => m.Id);
                
                   var res = await _commentsCollection
                   .WithReadConcern(new ReadConcern(ReadConcernLevel.Majority))
                   .Aggregate(PipelineDefinition<Comment, ReportProjection>.Create(pipeline))                 
                   .ToListAsync();

                for (int i = 0; i < res.Count; i++)
                {
                    result.Add(res[i]);
                }

                // // result = await _commentsCollection
                // //   .WithReadConcern(...)
                // //   .Aggregate()
                // //   .Group(...)
                // //   .Sort(...).Limt(...).Project(...).ToListAsync()                
                var top = new TopCommentsProjection(result);

                return top;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw;
            }
        }
    }
}
