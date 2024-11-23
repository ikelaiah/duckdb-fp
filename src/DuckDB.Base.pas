unit DuckDB.Base;

{$mode objfpc}{$H+}{$J-}
{$modeswitch advancedrecords}

interface   

uses
  SysUtils, Classes, Variants, DateUtils, libduckdb;

type
  EDuckDBError = class(Exception);

  TStringArray = array of string; 

  
  pduckdb_result = ^duckdb_result;

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

  // Forward declarations
  IDuckDBConnection = interface;
  IDuckFrame = interface;

 // Interface for DuckDB connection operations
  IDuckDBConnection = interface
    ['{4A8C9560-123A-4BCD-9EF0-A12B3C4D5E6F}']
    // Connection management
    procedure Open(const ADatabasePath: string = '');
    procedure Close;
    function Clone: IDuckDBConnection;

    
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
    // ... other TDuckDBConnection-related methods
  end;

  // Interface for DataFrame operations
  IDuckFrame = interface
    ['{F1B2C3D4-E5F6-47A8-B9C0-D1E2F3A4B5C6}']
    
    { Core: Column-related helper functions for data access and calculations }
    function GetColumnCount: Integer;
    function GetColumnNames: TStringArray;
    function GetColumn(Index: Integer): TDuckDBColumn;
    function GetColumnByName(const Name: string): TDuckDBColumn;
    function GetValue(Row, Col: Integer): Variant;
    function GetValueByName(Row: Integer; const ColName: string): Variant;
    function FindColumnIndex(const Name: string): Integer;
    function Select(const ColumnNames: TStringArray): IDuckFrame;  // Select columns

    { Core: DataFrame operations }
    procedure LoadFromResult(AResult: pduckdb_result);  // Load data from DuckDB result
    procedure Clear;                                    // Clear all data
    procedure Print(MaxRows: Integer = 10);             // Print DataFrame contents
    
    { Core: Value conversion helper }
    function TryConvertValue(const Value: Variant; FromType, ToType: TDuckDBColumnType): Variant;
    
    { Core: Union operations }
    function Union(const Other: IDuckFrame; Mode: TUnionMode = umStrict): IDuckFrame;
    function UnionAll(const Other: IDuckFrame; Mode: TUnionMode = umStrict): IDuckFrame;
    function Distinct: IDuckFrame;

    { IO: File-related operations }
    procedure SaveToCSV(const FileName: string);       // Export to CSV file

    { Data Preview: Methods for inspecting data samples }
    function Head(Count: Integer = 5): IDuckFrame;     // Get first N rows
    function Tail(Count: Integer = 5): IDuckFrame;     // Get last N rows

    { Data Analysis: Helper functions }
    function CalculateColumnStats(const Col: TDuckDBColumn): TColumnStats;
    function CalculatePercentile(const Values: array of Double; Percentile: Double): Double;

    { Data Analysis: Methods for examining data structure and statistics }
    procedure Describe;                                // Show statistical summary
    function NullCount: IDuckFrame;                    // Count null values per column
    procedure Info; 
    
    { Data Cleaning: Methods for handling missing data }
    function DropNA: IDuckFrame;                        // Remove rows with any null values
    function FillNA(const Value: Variant): IDuckFrame;  // Fill null values
    
    { Stats: Advanced analysis methods }
    function CorrPearson: IDuckFrame;
    function CorrSpearman: IDuckFrame;
    function UniqueCounts(const ColumnName: string): IDuckFrame; // Frequency of each unique value
    
    { Plot: ASCII plotting capabilities }
    procedure PlotHistogram(const ColumnName: string; Bins: Integer = 10);
                            
    { Methods for manual construction }
    procedure AddColumn(const AName: string; AType: TDuckDBColumnType);
    procedure AddRow(const AValues: array of Variant);
    procedure SetValue(const ARow: Integer; const AColnName: string;
                       const AValue: Variant);

    function Filter(const Condition: string): IDuckFrame;
    function Sort(const Columns: array of string): IDuckFrame;
    function Where(const Condition: string): IDuckFrame;
    // ... other DataFrame-related methods
  end;



  implementation

function CreateDuckDBConnection: IDuckDBConnection; forward;

{ TDuckDBColumn helper functions }

{ TDuckDBColumn helper functions }

{ Converts column data to array of Double values }
function TDuckDBColumn.AsDoubleArray: specialize TArray<Double>;
var
  i: Int64;
begin
  // Create result array with same length as data
  SetLength(Result, Length(Data));
  
  // Convert each element
  for i := 0 to High(Data) do
  begin
    if VarIsNull(Data[i]) then
      Result[i] := 0  // Convert NULL to 0 (could use NaN instead)
    else
      Result[i] := Double(Data[i]);  // Convert Variant to Double
  end;
end;

{ Converts column data to array of Integer values }
function TDuckDBColumn.AsIntegerArray: specialize TArray<Integer>;
var
  i: Int64;
begin
  // Create result array with same length as data
  SetLength(Result, Length(Data));
  
  // Convert each element
  for i := 0 to High(Data) do
  begin
    if VarIsNull(Data[i]) then
      Result[i] := 0  // Convert NULL to 0
    else
      Result[i] := Integer(Data[i]);  // Convert Variant to Integer
  end;
end;

{ Converts column data to array of Integer values }
function TDuckDBColumn.AsIntegerArray: specialize TArray<Int64>;
var
  i: Int64;
begin
  // Create result array with same length as data
  SetLength(Result, Length(Data));

  // Convert each element
  for i := 0 to High(Data) do
  begin
    if VarIsNull(Data[i]) then
      Result[i] := 0  // Convert NULL to 0
    else
      Result[i] := Int64(Data[i]);  // Convert Variant to Int64
  end;
end;

{ Converts column data to array of string values }
function TDuckDBColumn.AsStringArray: specialize TArray<string>;
var
  i: Int64;
begin
  // Create result array with same length as data
  SetLength(Result, Length(Data));
  
  // Convert each element
  for i := 0 to High(Data) do
  begin
    if VarIsNull(Data[i]) then
      Result[i] := ''  // Convert NULL to empty string
    else
      Result[i] := VarToStr(Data[i]);  // Convert Variant to string using VarToStr
  end;
end;

{ Converts column data to array of Boolean values }
function TDuckDBColumn.AsBooleanArray: specialize TArray<Boolean>;
var
  i: Int64;
begin
  // Create result array with same length as data
  SetLength(Result, Length(Data));
  
  // Convert each element
  for i := 0 to High(Data) do
  begin
    if VarIsNull(Data[i]) then
      Result[i] := False  // Convert NULL to False
    else
      Result[i] := Boolean(Data[i]);  // Convert Variant to Boolean
  end;
end;

function TDuckDBColumn.AsDateArray: specialize TArray<TDate>;
var
  i: Integer;
  TempDateTime: TDateTime;
begin
  SetLength(Result, Length(Data));
  for i := 0 to High(Data) do
  begin
    if VarIsNull(Data[i]) then
      Result[i] := 0  // Default date (30/12/1899)
    else if VarIsType(Data[i], varDate) then
      Result[i] := VarToDateTime(Data[i])  // Keep the raw date value
    else if TryStrToDateTime(VarToStr(Data[i]), TempDateTime) then
      Result[i] := Int(TempDateTime)
    else
      Result[i] := TDate(VarToDateTime(Data[i]));  // Use the raw value directly
  end;
end;

{ Converts column data to array of TTime values 
  Null values are converted to 0 (which is 00:00:00). 
  Note: In Pascal/Delphi, a TDateTime value stores the date in the 
        integer portion and the time in the fractional portion, so 
        Frac() is also a correct way to extract just the time component. }
function TDuckDBColumn.AsTimeArray: specialize TArray<TTime>;
var
  i: Integer;
  TempDateTime: TDateTime;
begin
  SetLength(Result, Length(Data));
  for i := 0 to High(Data) do
  begin
    if VarIsNull(Data[i]) then
      Result[i] := 0
    else if VarIsType(Data[i], varDate) then
      Result[i] := VarToDateTime(Data[i])  // Keep the raw time value
    else if TryStrToTime(VarToStr(Data[i]), TempDateTime) then
      Result[i] := TempDateTime
    else
      Result[i] := TTime(VarToDateTime(Data[i]));  // Use the raw value directly
  end;
end;

function TDuckDBColumn.AsDateTimeArray: specialize TArray<TDateTime>;
var
  i: Integer;
begin
  SetLength(Result, Length(Data));
  for i := 0 to High(Data) do
  begin
    if VarIsNull(Data[i]) then
      Result[i] := 0  // Default date/time (30/12/1899 00:00:00)
    else if VarIsType(Data[i], varDate) then
      Result[i] := VarToDateTime(Data[i])
    else
      Result[i] := StrToDateTime(VarToStr(Data[i]));
  end;
end;

  end.
