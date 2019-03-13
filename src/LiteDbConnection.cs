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

        public override sealed IEnumerable<T> Get<T>()
        {
            using (var db = new LiteRepository(DbPath))
            {
                return db.Fetch<T>();
            }
        }

        public override sealed IEnumerable<T> Get<T>(Expression<Func<T, bool>> query)
        {
            using (var db = new LiteRepository(DbPath))
            {
                return db.Query<T>().Where(query).ToArray();
            }
        }

        public override sealed void Insert<T>(T dto)
        {
            using (var db = new LiteRepository(DbPath))
            {
                db.Insert<T>(dto);
            }
        }

        public override sealed void Update<T>(IEnumerable<T> dtos)
        {
            using (var db = new LiteRepository(DbPath))
            {
                db.Update<T>(dtos);
            }
        }

        public override sealed bool Update<T>(T dto)
        {
            using (var db = new LiteRepository(DbPath))
            {
                return db.Update<T>(dto);
            }
        }

        public override sealed bool Upsert<T>(T dto)
        {
            using (var db = new LiteRepository(DbPath))
            {
                return db.Upsert<T>(dto);
            }
        }

        public override sealed bool Delete<T>(Guid id)
        {
            using (var db = new LiteRepository(DbPath))
            {
                return db.Delete<T>(id);
            }
        }

        public override sealed void DeleteAll<T>()
        {
            using (var db = new LiteRepository(DbPath))
            {
                var ids = db.Fetch<T>().Select(q => q.Id);
                foreach (var id in ids)
                    db.Delete<T>(id);
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
