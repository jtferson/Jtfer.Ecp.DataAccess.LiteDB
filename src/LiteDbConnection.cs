using LiteDB;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Jtfer.Ecp.DataAccess.LiteDB
{
    public abstract class LiteDbConnection : DbConnectionBase
    {
        private Dictionary<string, Type> _mappedTypes = new Dictionary<string, Type>();

        protected abstract bool Journal { get; }

        private ConnectionString _connectionStringCache;
        private ConnectionString _connectionString => _connectionStringCache = _connectionStringCache ?? GetConnectionString();
        private ConnectionString GetConnectionString()
        {
            return new ConnectionString($"Filename={DbPath};Journal={Journal}");
        }


        public override sealed IEnumerable<T> Get<T>()
        {
            var connection = _connectionString;
            connection.Mode = FileMode.ReadOnly;



            using (var db = new LiteRepository(connection))
            {
                return db.Fetch<T>();
            }
        }

        public override sealed IEnumerable<T> Get<T>(Expression<Func<T, bool>> query)
        {
            var connection = _connectionString;
            connection.Mode = FileMode.ReadOnly;

            using (var db = new LiteRepository(connection))
            {
                return db.Query<T>().Where(query).ToArray();
            }
        }

        public override sealed void Insert<T>(T dto)
        {
            var connection = _connectionString;
            connection.Mode = FileMode.Shared;

            using (var db = new LiteRepository(connection))
            {
                db.Insert<T>(dto);
            }
        }
        public override sealed void Insert<T>(IEnumerable<T> dtos)
        {
            var connection = _connectionString;
            connection.Mode = FileMode.Shared;

            using (var db = new LiteRepository(connection))
            {
                db.Insert<T>(dtos);
            }
        }
        public override sealed void Update<T>(IEnumerable<T> dtos)
        {
            var connection = _connectionString;
            connection.Mode = FileMode.Shared;

            using (var db = new LiteRepository(connection))
            {
                db.Update<T>(dtos);
            }
        }

        public override sealed bool Update<T>(T dto)
        {
            var connection = _connectionString;
            connection.Mode = FileMode.Shared;

            using (var db = new LiteRepository(connection))
            {
                return db.Update<T>(dto);
            }
        }

        public override sealed bool Upsert<T>(T dto)
        {
            var connection = _connectionString;
            connection.Mode = FileMode.Shared;

            using (var db = new LiteRepository(connection))
            {
                return db.Upsert<T>(dto);
            }
        }

        public override sealed bool Delete<T>(Guid id)
        {
            var connection = _connectionString;
            connection.Mode = FileMode.Shared;

            using (var db = new LiteRepository(connection))
            {
                return db.Delete<T>(id);
            }
        }

        public override sealed void DeleteAll<T>()
        {
            var connection = _connectionString;
            connection.Mode = FileMode.Shared;

            using (var db = new LiteRepository(connection))
            {
                db.Database.DropCollection(typeof(T).Name);
                //var ids = db.Fetch<T>().Select(q => q.Id);
                //foreach (var id in ids)
                //    db.Delete<T>(id);
            }
        }


        public override sealed void MapEntityToTable<T>()
        {
            var mapper = BsonMapper.Global;
            mapper.Entity<T>();
            _mappedTypes.Add(typeof(T).Name, typeof(T));
        }

        public override sealed IEnumerable<Type> GetMappedTypes()
        {
            return _mappedTypes.Values;
        }

        public override sealed void RunInTransaction(Action transaction)
        {
            if (transaction != null)
                transaction();
        }
    }
}
