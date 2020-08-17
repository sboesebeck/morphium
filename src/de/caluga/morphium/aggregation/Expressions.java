package de.caluga.morphium.aggregation;

public enum Expressions {
    //Arithmetics
    abs, add, ceil, divide, exp, floor, ln, log, log10, mod, multiply, pow, round, sqrt, subtract, trunc,
    //Array Expression Operators
    arrayElemAt, arrayToObject, concatArrays, filter, first, in, indexOfArray, isArray, last,
    map, objectToArray, range, reduce, reverseArray, size, slice, zip,

    //Boolean Expression Operators
    and, or, not,

    //Comparison Expression Operators
    cmp, eq, ne, gt, lt, gte, lte,

    //Conditional Expression Operators
    cond, ifNull, switchExpr,

    //Custom Aggregation
    function, accumulator,

    //Data Size Operators
    binarySize, bsonSize,

    //Date Expression Operators

    dateFromParts, dateFromString, dateToParts, dateToString, dayOfMonth, dayOfWeek, dayOfYear, hour,
    isoDayOfWeek, isoWeek, isoWeekYear, millisecond, minute, month, second, toDate,
    week, year,

    literal,


    mergeObjects,

    //Set Expression Operators

    allElementsTrue, anyElementTrue, setDifference, setEquals, setIntersection, setIsSubset, setUnion,

    //concat

    concat, dindexOfBytes, indexOfCP, ltrim, regexFind, regexFindAll, regexMatch,
    replaceOne, replaceAll, rtrim, split, strLenBytes, strLenCP, strcasecmp,
    substr, substrBytes, substrCP, toLower, toString, trim, toUpper,

    meta,

    //Trigonometry Expression Operators

    sin, $cos, tan, asin, acos, atan, atan2, asinh, acosh, atanh, degreesToRadian, radiansToDegrees,

    //Type Expression

    convert, isNumber, toBool, toDecimal, toDouble, toInt, toLong, toObjectId, type,

    //Group stage

    addToSet, avg, max, min, push, stdDevPop, stdDevSamp, sum,

    let,

}
