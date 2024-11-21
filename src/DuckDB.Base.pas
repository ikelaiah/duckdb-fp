unit DuckDB.Base;

{$mode objfpc}{$H+}{$J-}

interface   

uses
  SysUtils, Classes, Variants, Math, DateUtils, libduckdb;

type
  EDuckDBError = class(Exception);

  TStringArray = array of string; 

  // Forward declarations
  IDuckDBConnection = interface;
  IDuckFrame = interface;

 // Interface for DuckDB connection operations
  IDuckDBConnection = interface
    ['{4A8C9560-123A-4BCD-9EF0-A12B3C4D5E6F}']
    // Connection management
    procedure Open(const ADatabasePath: string = '');
    procedure Close;
    function Clone: TDuckDBConnection;
    property IsConnected: Boolean read FIsConnected;
    property DatabasePath: string read FDatabasePath;
    
    // Query execution
    procedure ExecuteSQL(const ASQL: string);
    function Query(const ASQL: string): IDuckFrame;
    function QueryValue(const ASQL: string): Variant;
    
    // Transaction management
    procedure BeginTransaction;
    procedure Commit;
    procedure Rollback;
    
    function ReadCSV(const FileName: string): IDuckFrame;
    procedure WriteToTable(const DataFrame: IDuckFrame; const TableName: string; 
      const SchemaName: string = 'main');
  end;

  // Interface for DataFrame operations
  IDuckFrame = interface
    ['{F1B2C3D4-E5F6-47A8-B9C0-D1E2F3A4B5C6}']
    function Filter(const Condition: string): IDuckFrame;
    function Sort(const Columns: array of string): IDuckFrame;
    function Where(const Condition: string): IDuckFrame;
    // ... other DataFrame-related methods
  end;


  {
  Union modes

  1. umStrict - Most conservative mode
    - Requires exact match of column names and types
    - Ensures data consistency
    - Best for when you know both DataFrames should have identical structure
    - Similar to SQL's UNION with strict type checking
  2. umCommon - Intersection mode
    - Only includes columns that exist in both DataFrames
    - Flexible when DataFrames have different structures but share some columns
    - Similar to SQL's NATURAL JOIN behavior
    - Good for when you want to safely combine DataFrames with different schemas
  3. umAll - Most inclusive mode
    - Includes all columns from both DataFrames
    - Missing values are filled with NULL
    - Most flexible but might need careful handling of NULL values
    - Similar to SQL's FULL OUTER JOIN concept
    - Useful when you want to preserve all data from both frames
  }
  TUnionMode = (
    umStrict,    // Strict mode: columns must match exactly
    umCommon,    // Common columns: only include columns that appear in both frames
    umAll        // All columns: include all columns from both frames
  );

  { Column types that map to DuckDB's native types }
  TDuckDBColumnType = (
    dctUnknown,    // Unknown or unsupported type
    dctBoolean,    // Boolean (true/false)
    dctTinyInt,    // 8-bit integer
    dctSmallInt,   // 16-bit integer
    dctInteger,    // 32-bit integer
    dctBigInt,     // 64-bit integer
    dctFloat,      // Single-precision floating point
    dctDouble,     // Double-precision floating point
    dctDate,       // Date without time (YYYY-MM-DD)
    dctTime,       // Time without date (HH:MM:SS.SSS)
    dctTimestamp,  // Date with time (YYYY-MM-DD HH:MM:SS.SSS)
    dctInterval,   // Time interval/duration
    dctString,     // Variable-length string
    dctBlob,       // Binary large object
    dctDecimal,    // Decimal number with precision and scale
    dctUUID,       // Universally Unique Identifier
    dctJSON        // JSON data
  );

  { TDuckDBColumn represents a single column in a DuckDB DataFrame }
  TDuckDBColumn = record
    Name: string;           // Column name from query or user definition
    DataType: TDuckDBColumnType;  // Column's data type (Integer, Double, etc.)
    Data: array of Variant; // Raw column data stored as Variants for flexibility
    
    { Helper functions to return strongly-typed arrays }
    
    { Converts the column data to an array of Double values.
      Null values are converted to 0. }
    function AsDoubleArray: specialize TArray<Double>;
    
    { Converts the column data to an array of Integer values.
      Null values are converted to 0. }
    function AsIntegerArray: specialize TArray<Integer>;

    { Converts the column data to an array of Int64 values.
      Null values are converted to 0. }
    function AsIntegerArray: specialize TArray<Int64>;
    
    { Converts the column data to an array of string values.
      Null values are converted to empty strings. }
    function AsStringArray: specialize TArray<string>;
    
    { Converts the column data to an array of Boolean values.
      Null values are converted to False. }
    function AsBooleanArray: specialize TArray<Boolean>;

    { Converts column data to array of TDate values.
      Null values are converted to 0 (which is 30/12/1899 in TDate). }
    function AsDateArray: specialize TArray<TDate>;
    
    { Converts column data to array of TTime values.
      Null values are converted to 0 (which is 00:00:00). }
    function AsTimeArray: specialize TArray<TTime>;

    { Converts column data to array of TDateTime values.
    Null values are converted to 0 (which is 30/12/1899 in TDateTime). }
    function AsDateTimeArray: specialize TArray<TDateTime>;
  end;

  { Statistical measures for numeric columns }
  TColumnStats = record
    Count: Integer;         // Total number of rows
    Mean: Double;           // Average value
    StdDev: Double;         // Standard deviation
    Skewness: Double;       // Measure of distribution asymmetry (0 is symmetric)
    Kurtosis: Double;       // Measure of "tailedness" compared to normal distribution
    NonMissingRate: Double; // Percentage of non-null values (1.0 = no nulls)
    Min: Variant;           // Minimum value
    Q1: Double;             // First quartile (25th percentile)
    Median: Double;         // Median (50th percentile)
    Q3: Double;             // Third quartile (75th percentile)
    Max: Variant;           // Maximum value
    NullCount: Integer;     // Number of null values
    Ordered: Boolean;       // Is the data ordered?
    NUnique: Integer;       // Number of unique values
    TopCounts: string;      // Most frequent values and their counts
  end;

  implementation

  end.