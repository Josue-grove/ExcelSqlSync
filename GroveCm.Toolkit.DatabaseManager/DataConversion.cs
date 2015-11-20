using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using Newtonsoft.Json;

namespace GroveCm.Toolkit.DatabaseManager
{
    public static class DataConversion
    {
        public const string DateTimeFormat = "yyyy-MM-ddTHH:mm:ss";

        static Dictionary<string, Func<object, dynamic>> CustomConvertions { get; set; }

        static DataConversion()
        {
            CustomConvertions = new Dictionary<string, Func<object, dynamic>>();
        }

        /// <summary>
        /// Convert a given object into type of T
        /// </summary>
        /// <typeparam name="T">The type to convert the object into</typeparam>
        /// <param name="input">The given object</param>
        /// <returns></returns>
        public static T ConvertTo<T>(this object input)
        {
            var output = default(T);

            if (input != null && input != DBNull.Value && !(typeof(T) != typeof(string) && input.ToString() == ""))
            {
                if (typeof(T) == typeof(byte) || typeof(T) == typeof(byte?))
                {
                    output = (T)(object)Convert.ToByte(input);
                }
                else if (typeof(T) == typeof(int) || typeof(T) == typeof(int?))
                {
                    output = (T)(object)Convert.ToInt32(input);
                }
                else if (typeof(T) == typeof(long) || typeof(T) == typeof(long?))
                {
                    output = (T)(object)Convert.ToInt64(input);
                }
                else if (typeof(T) == typeof(double) || typeof(T) == typeof(double?))
                {
                    output = (T)(object)Convert.ToDouble(input);
                }
                else if (typeof(T) == typeof(decimal) || typeof(T) == typeof(decimal?))
                {
                    output = (T)(object)Convert.ToDecimal(input);
                }
                else if (typeof(T) == typeof(DateTime) && input is string)
                {
                    output = (T)(object)DateTime.ParseExact((string)input, DateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);
                }
                else if (typeof(T) == typeof(string) && input is DateTime)
                {
                    output = (T)(object)(((DateTime)input).ToString(DateTimeFormat));
                }
                else
                {
                    output = (T)Convert.ChangeType(input, typeof(T));
                }
            }

            return output;
        }

        /// <summary>
        /// Deep clone a given object
        /// </summary>
        /// <typeparam name="T">Object type</typeparam>
        /// <param name="value">Object to clone</param>
        /// <returns>A new cloned object of the type T</returns>
        public static T DeepClone<T>(this T value)
        {
            var serialized = JsonConvert.SerializeObject(value);

            return JsonConvert.DeserializeObject<T>(serialized);
        }

        /// <summary>
        /// Convert a given object of type T1 into another object of type T2 by mapping their properties by name
        /// </summary>
        /// <typeparam name="T1">Source object type</typeparam>
        /// <typeparam name="T2">Target object type</typeparam>
        /// <param name="value">The source object of type T1</param>
        /// <returns>A new object of type T2 with all the matching properties assigned</returns>
        public static T2 DeepConvert<T1, T2>(T1 value)
        {
            var serialized = JsonConvert.SerializeObject(value);

            return JsonConvert.DeserializeObject<T2>(serialized);
        }

        /// <summary>
        /// Convert a given object into type T
        /// </summary>
        /// <typeparam name="T">The type to convert into</typeparam>
        /// <param name="value">The object to convert</param>
        /// <returns>A new object of type T with all the matching properties assigned</returns>
        public static T DeepConvert<T>(this object value)
        {
            return DeepConvert<object, T>(value);
        }


        /// <summary>
        /// Given a data reader, returns its default mapping dictionary. It can be customized later on to map the right property names
        /// </summary>
        /// <param name="dataReader">The given data reader</param>
        /// <returns>A dictionary of string keys and int values:
        /// - string key: the non sensitive name of the column
        /// - int value: the ordinal number of the column of the given IDataRecord
        /// </returns>
        public static Dictionary<string, int> GetMappingDictionary(this IDataReader dataReader)
        {
            var mapping = new Dictionary<string, int>();

            for (int i = 0; i < dataReader.FieldCount; i++)
            {
                mapping.Add(dataReader.GetName(i).ToLower(), i);
            }

            return mapping;
        }


        /// <summary>
        /// Adds a custom conversion to be used by RowToType when mapping a property of the target type name
        /// </summary>
        /// <param name="targetTypeName">The target type name</param>
        /// <param name="conversionFunction">The conversion function of object, dynamic signature:
        /// - object: the column to convert from a IDataRecord
        /// - dynamic: a function that returns the new type
        /// Example: DataAdapter.AddCustomConversion("Geometry", value => new Geometry(value));
        /// </param>
        public static void AddCustomConversion(string targetTypeName, Func<object, dynamic> conversionFunction)
        {
            CustomConvertions.Add(targetTypeName, conversionFunction);
        }


        /// <summary>
        /// Given an IDataRecord, converts its content into type of T by creating a new instance of T and populating it by mapping the properties
        /// of T to the columns of the IDataRecord. This method does not map the fields of T. Only mapped properties get populated.
        /// If mapping is provided as null the columns and properties will be mapped by non-sensitive name. This will be done just the first time the method is called
        /// with the same mapping dictionary, so performance cost would be minimal.
        /// Custom type conversions for casting properties of T can be injected using AddCustomConversion 
        /// </summary>
        /// <typeparam name="T">The type of the object we want to produce from the IDataRow</typeparam>
        /// <param name="dr">The data row containing the data to populate the new object</param>
        /// <param name="mapping">A dictionary of string keys and int values:
        /// - string key: the non sensitive name of the column
        /// - int value: the ordinal number of the column of the given IDataRecord
        /// </param>
        /// <returns>A new object of type T populated with the data from the given IDataRecord</returns>
        public static T RowToType<T>(this IDataRecord dr, ref Dictionary<string, int> mapping)
        {
            if (mapping == null)
            {
                mapping = new Dictionary<string, int>();

                for (int i = 0; i < dr.FieldCount; i++)
                {
                    mapping.Add(dr.GetName(i).ToLower(), i);
                }
            }

            var output = Activator.CreateInstance<T>();

            foreach (PropertyInfo prop in output.GetType().GetProperties())
            {
                int foundColumn;
                if (mapping.TryGetValue(prop.Name.ToLower(), out foundColumn))
                {
                    if (!Equals(dr[foundColumn], DBNull.Value))
                    {
                        Func<object, dynamic> conversionFunction;
                        if (CustomConvertions.TryGetValue(prop.PropertyType.Name, out conversionFunction))
                        {
                            prop.SetValue(output, conversionFunction(dr[foundColumn]));
                        }
                        else
                        {
                            if (prop.PropertyType == typeof(long) || prop.PropertyType == typeof(long?))
                            {
                                prop.SetValue(output, Convert.ToInt64(dr[foundColumn]));
                            }
                            else if (prop.PropertyType == typeof(double) || prop.PropertyType == typeof(double?))
                            {
                                prop.SetValue(output, Convert.ToDouble(dr[foundColumn]));
                            }
                            else if (prop.PropertyType == typeof(byte) || prop.PropertyType == typeof(byte?))
                            {
                                prop.SetValue(output, Convert.ToByte(dr[foundColumn]));
                            }
                            else if (prop.PropertyType == typeof(Int16) || prop.PropertyType == typeof(Int16?))
                            {
                                prop.SetValue(output, Convert.ToInt16(dr[foundColumn]));
                            }
                            else
                            {
                                prop.SetValue(output, dr[foundColumn]);
                            }
                        }
                    }
                }
            }

            return output;
        }


        /// <summary>
        /// Given an IDataRecord, converts its content into type of T by creating a new instance of T and populating it by mapping the properties
        /// of T to the columns of the IDataRecord. This method does not map the fields of T. Only mapped properties get populated.
        /// Custom type conversions for casting properties of T can be injected using AddCustomConversion 
        /// </summary>
        /// <typeparam name="T">The type of the object we want to produce from the IDataRow</typeparam>
        /// <param name="dr">The data row containing the data to populate the new object</param>
        /// <param name="mapping">A dictionary of string keys and int values:
        /// - string key: the non sensitive name of the T property
        /// - int value: the ordinal number of the column of the given IDataRecord
        /// </param>
        /// <returns>A new object of type T populated with the data from the given IDataRecord</returns>
        public static T RowToType<T>(this IDataRecord dr, Dictionary<string, int> mapping)
        {
            var mappingLower = mapping.ToDictionary(map => map.Key.ToLower(), map => map.Value);
            return RowToType<T>(dr, ref mappingLower);
        }


        /// <summary>
        /// Given an IDataRecord, converts its content into type of T by creating a new instance of T and populating it by mapping the properties
        /// of T to the columns of the IDataRecord. This method does not map the fields of T. Only mapped properties get populated.
        /// As mapping is not provided the columns and properties will be mapped by non-sensitive name. This will be done every time the method is called so it would have a
        /// performance cost. 
        /// Custom type conversions for casting properties of T can be injected using AddCustomConversion.
        /// </summary>
        /// <typeparam name="T">The type of the object we want to produce from the IDataRow</typeparam>
        /// <param name="dr">The data row containing the data to populate the new object</param>
        /// <returns>A new object of type T populated with the data from the given IDataRecord</returns>
        public static T RowToType<T>(this IDataRecord dr)
        {
            Dictionary<string, int> mapping = null;
            return RowToType<T>(dr, ref mapping);
        }


        /// <summary>
        /// Converts a set of int into a string of comma separated values, typically to be use as part of a T-SQL script
        /// </summary>
        /// <param name="values">The set of values</param>
        /// <returns>A string of comma separated values</returns>
        public static string ToCommaSeparatedSqlString(this IEnumerable<int> values)
        {
            var outputBuilder = new StringBuilder();
            foreach (var value in values)
            {
                outputBuilder.Append(string.Format("{0}{1}", outputBuilder.Length > 0 ? "," : "", value));
            }
            return outputBuilder.ToString();
        }


        /// <summary>
        /// For a given IDataReader, provides a way of retrieve its rows as an IEnumerable.
        /// However, caution is advised as the sequential nature of the data reader would not work in some scenarios, like Parallel.ForEach and some Linq expressions
        /// </summary>
        /// <param name="reader"></param>
        /// <returns>An IEnumerable to enumerate the data reader rows</returns>
        public static IEnumerable<IDataRecord> Rows(this IDataReader reader)
        {
            while (reader.Read())
            {
                yield return reader;
            }
        }


        /// <summary>
        /// Calculates the MD5 hash of a given string
        /// </summary>
        /// <param name="input">The given string</param>
        /// <returns>The MD5 hash as a hex string</returns>
        public static string CalculateMd5Hash(this string input)
        {
            // step 1, calculate MD5 hash from input
            var md5 = MD5.Create();
            var inputBytes = Encoding.ASCII.GetBytes(input);
            var hash = md5.ComputeHash(inputBytes);

            // step 2, convert byte array to hex string
            var sb = new StringBuilder();
            for (int i = 0; i < hash.Length; i++)
            {
                sb.Append(hash[i].ToString("X2"));
            }
            return sb.ToString();
        }
    }
}
