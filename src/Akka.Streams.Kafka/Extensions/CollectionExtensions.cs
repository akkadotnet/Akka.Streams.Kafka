using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Akka.Streams.Kafka.Extensions
{
    public static class CollectionExtensions
    {
        /// <summary>
        /// Joins elements of the collection with given separator to single string
        /// </summary>
        public static string JoinToString<T>(this IEnumerable<T> collection, string separator)
        {
            return string.Join(separator, collection);
        }

        /// <summary>
        /// Converts dictionary to a list of tuples
        /// </summary>
        public static IEnumerable<(TKey, TValue)> ToTuples<TKey, TValue>(this IEnumerable<KeyValuePair<TKey, TValue>> dictionary)
        {
            return dictionary.Select(pair => (pair.Key, pair.Value));
        }

        /// <summary>
        /// Checks if collection is empty
        /// </summary>
        public static bool IsEmpty<T>(this IEnumerable<T> collection)
        {
            return !collection.Any();
        }

        /// <summary>
        /// Converts collection to <see cref="IImmutableSet{T}"/>
        /// </summary>
        public static IImmutableSet<T> ToImmutableSet<T>(this IEnumerable<T> collection)
        {
            return collection.ToImmutableHashSet();
        }
        
        /// <summary>
        /// Split a list into two list depending on the boolean return value of the predicate.
        /// </summary>
        public static (List<T> True, List<T> False) Partition<T>(this List<T> list, Predicate<T> predicate)
        {
            var left = new List<T>();
            var right = new List<T>();
            foreach (var t in list)
            {
                if(predicate(t))
                    left.Add(t);
                else
                    right.Add(t);
            }
        
            return (left, right);
        }        
    }
}