unit DuckDB.DataFrame;

{$mode objfpc}{$H+}{$J-}
{$modeswitch advancedrecords}
interface

uses
  SysUtils, Classes, Variants, libduckdb, Math, TypInfo, Generics.Collections, DateUtils;

const
  Infinity: Double = 1.0/0.0;  // Represents positive infinity

type
  EDuckDBError = class(Exception);

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


  TJoinMode = (
    jmInner,     // Only matching rows from both frames
    jmLeftJoin,  // All rows from left frame, matching from right
    jmRightJoin, // All rows from right frame, matching from left
    jmFullJoin   // All rows from both frames
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

  TStringArray = array of string;  // Add this type declaration at the unit level

  { TJoinColumnMap is used to map columns from the left and right DataFrames 
    during a join operation }
  TJoinColumnMap = record
    LeftIndex: Integer;
    RightIndex: Integer;
  end;


  { DataFrame class for handling query results in DuckDB compatible datatype}
  TDuckFrame = class
  private
    FColumns: array of TDuckDBColumn;  // Array of columns
    FRowCount: Integer;                // Number of rows in the DataFrame
    
    { Core: Union-related helper functions for combining dataframes }
    function GetCommonColumns(const Other: TDuckFrame): TStringArray;
    function GetAllColumns(const Other: TDuckFrame): TStringArray;
    function HasSameStructure(const Other: TDuckFrame): Boolean;

    { Stats: Type mapping and statistical calculations }
    function MapDuckDBType(duckdb_type: duckdb_type): TDuckDBColumnType;
    function IsNumericColumn(const Col: TDuckDBColumn): Boolean;

    { Private helpers for constructors }
    procedure InitializeBlank(const AColumnNames: array of string; 
                            const AColumnTypes: array of TDuckDBColumnType);

    { Private helpers for Join }
  { Private helpers for Join }
  procedure PerformInnerJoin(Result: TDuckFrame; Other: TDuckFrame; 
    const CommonCols: array of string; const ColMap: array of TJoinColumnMap);
  procedure PerformLeftJoin(Result: TDuckFrame; Other: TDuckFrame; 
    const CommonCols: array of string; const ColMap: array of TJoinColumnMap);
  procedure PerformRightJoin(Result: TDuckFrame; Other: TDuckFrame; 
    const CommonCols: array of string; const ColMap: array of TJoinColumnMap);
  procedure PerformFullJoin(Result: TDuckFrame; Other: TDuckFrame; 
    const CommonCols: array of string; const ColMap: array of TJoinColumnMap);

  public

    { Default Constructor and destructor }
    constructor Create;
    destructor Destroy; override;
    
    { Core: Column-related helper functions for data access and calculations }
    function GetColumnCount: Integer;
    function GetColumnNames: TStringArray;
    function GetColumn(Index: Integer): TDuckDBColumn;
    function GetColumnByName(const Name: string): TDuckDBColumn;
    function GetValue(Row, Col: Integer): Variant;
    function GetValueByName(Row: Integer; const ColName: string): Variant;
    function FindColumnIndex(const Name: string): Integer;
    function Select(const ColumnNames: array of string): TDuckFrame;  // Select columns

    { Core: DataFrame operations }
    procedure LoadFromResult(AResult: pduckdb_result);  // Load data from DuckDB result
    procedure Clear;                                    // Clear all data
    procedure Print(MaxRows: Integer = 10);             // Print DataFrame contents
    
    { Core: Value conversion helper }
    function TryConvertValue(const Value: Variant; FromType, ToType: TDuckDBColumnType): Variant;
    
    { Core: Union operations }
    function Union(const Other: TDuckFrame; Mode: TUnionMode = umStrict): TDuckFrame;
    function UnionAll(const Other: TDuckFrame; Mode: TUnionMode = umStrict): TDuckFrame;
    function Distinct: TDuckFrame;

    { IO: File-related operations }
    procedure SaveToCSV(const FileName: string);       // Export to CSV file

    { Data Preview: Methods for inspecting data samples }
    function Head(Count: Integer = 5): TDuckFrame;     // Get first N rows
    function Tail(Count: Integer = 5): TDuckFrame;     // Get last N rows

    { Data Analysis: Helper functions }
    function CalculateColumnStats(const Col: TDuckDBColumn): TColumnStats;
    function CalculatePercentile(const Values: array of Double; Percentile: Double): Double;

    { Data Analysis: Methods for examining data structure and statistics }
    procedure Describe;                                // Show statistical summary
    function NullCount: TDuckFrame;                    // Count null values per column
    procedure Info; 
    
    { Data Cleaning: Methods for handling missing data }
    function DropNA: TDuckFrame;                        // Remove rows with any null values
    function FillNA(const Value: Variant): TDuckFrame;  // Fill null values
    
    { Data Manipulation: Methods for filtering and transforming data 
      Added part of 0.1.0 }
    function Filter(const ColumnName: string; const Value: Variant): TDuckFrame; overload;
    function Filter(const ColumnName: string; const CompareOp: string; const Value: Variant): TDuckFrame; overload;
    function Sort(const ColumnName: string; Ascending: Boolean = True): TDuckFrame; overload;
    function Sort(const ColumnNames: array of string; const Ascending: array of Boolean): TDuckFrame; overload;
    function GroupBy(const ColumnNames: array of string): TDuckFrame;
    function Sample(Count: Integer): TDuckFrame; overload;
    function Sample(Percentage: Double): TDuckFrame; overload;
    function RenameColumn(const OldName, NewName: string): TDuckFrame;
    function DropColumns(const ColumnNames: array of string): TDuckFrame;

    { Stats: Advanced analysis methods }
    function CorrPearson: TDuckFrame;
    function CorrSpearman: TDuckFrame;
    function UniqueCounts(const ColumnName: string): TDuckFrame; // Frequency of each unique value
    
    { Plot: ASCII plotting capabilities }
    procedure PlotHistogram(const ColumnName: string; Bins: Integer = 10);
    
    { Properties }
    property RowCount: Integer read FRowCount;
    property ColumnCount: Integer read GetColumnCount;
    property Columns[Index: Integer]: TDuckDBColumn read GetColumn;
    property ColumnsByName[const Name: string]: TDuckDBColumn read GetColumnByName;
    property Values[Row, Col: Integer]: Variant read GetValue;
    property ValuesByName[Row: Integer; const ColName: string]: Variant read GetValueByName; default;

    { Constructors: More Constructors }
    constructor CreateBlank(const AColumnNames: array of string;
                           const AColumnTypes: array of TDuckDBColumnType); overload;
    constructor CreateFromDuckDB(const ADatabase, ATableName: string); overload;
    constructor CreateFromCSV(const AFileName: string; 
                              const AHasHeaders: Boolean = True;
                              const ADelimiter: Char = ','); overload;
    constructor CreateFromParquet(const AFileName: string); overload;
    constructor CreateFromParquet(const Files: array of string); overload;
                            
    { Methods for manual construction }
    procedure AddColumn(const AName: string; AType: TDuckDBColumnType);
    procedure AddRow(const AValues: array of Variant);
    procedure SetValue(const ARow: Integer; const AColumnName: string; 
                       const AValue: Variant);

    function ValueCounts(const ColumnName: string; 
      Normalize: Boolean = False): TDuckFrame;
    function Join(Other: TDuckFrame; Mode: TJoinMode = jmLeftJoin): TDuckFrame;
    function Quantile(const ColumnName: string; const Quantiles: array of Double): TDuckFrame;


  end;

{ Helper function for sorting }
procedure QuickSort(var A: array of Double; iLo, iHi: Integer; Ascending: Boolean = True);
procedure QuickSortWithIndices(var Values: array of Double; var Indices: array of Integer; Left, Right: Integer; Ascending: Boolean = True);
procedure QuickSortWithStrings(var Values: array of string; var Indices: array of Integer; Left, Right: Integer; Ascending: Boolean = True);

{ Converts a DuckDB column type to its SQL string representation }
function DuckDBTypeToString(ColumnType: TDuckDBColumnType): string;

{ Converts a SQL type string to its DuckDB column type }
function StringToDuckDBType(const TypeName: string): TDuckDBColumnType;

{ Helper function to check if an array contains a specific value }
function Contains(const Arr: array of string; const Value: string): Boolean;  

implementation

function Contains(const Arr: array of string; const Value: string): Boolean;
var
  I: Integer;
begin
  Result := False;
  for I := 0 to High(Arr) do
    if Arr[I] = Value then
      Exit(True);
end;

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

{ TDuckFrame }

constructor TDuckFrame.Create;
begin
  inherited Create;
  FRowCount := 0;
  SetLength(FColumns, 0);
end;

destructor TDuckFrame.Destroy;
begin
  Clear;
  inherited;
end;

function TDuckFrame.GetColumnCount: Integer;
begin
  Result := Length(FColumns);
end;

function TDuckFrame.GetColumn(Index: Integer): TDuckDBColumn;
begin
  if (Index < 0) or (Index >= Length(FColumns)) then
    raise EDuckDBError.Create('Column index out of range');
  Result := FColumns[Index];
end;

function TDuckFrame.GetColumnByName(const Name: string): TDuckDBColumn;
var
  Index: Integer;
begin
  Index := FindColumnIndex(Name);
  if Index = -1 then
    raise EDuckDBError.CreateFmt('Column "%s" not found', [Name]);
  Result := FColumns[Index];
end;

function TDuckFrame.GetValue(Row, Col: Integer): Variant;
begin
  if (Row < 0) or (Row >= FRowCount) then
    raise EDuckDBError.Create('Row index out of range');
  if (Col < 0) or (Col >= Length(FColumns)) then
    raise EDuckDBError.Create('Column index out of range');
  Result := FColumns[Col].Data[Row];
end;

function TDuckFrame.GetValueByName(Row: Integer; const ColName: string): Variant;
var
  ColIndex: Integer;
begin
  ColIndex := FindColumnIndex(ColName);
  if ColIndex = -1 then
    raise EDuckDBError.CreateFmt('Column "%s" not found', [ColName]);
  Result := GetValue(Row, ColIndex);
end;

function TDuckFrame.FindColumnIndex(const Name: string): Integer;
var
  I: Integer;
begin
  for I := 0 to Length(FColumns) - 1 do
    if FColumns[I].Name = Name then
      Exit(I);
  Result := -1;
end;

procedure TDuckFrame.Clear;
var
  i:Integer;
begin
  // Don't clear FColumns, only clear the data
  for i := 0 to Length(FColumns) - 1 do
    SetLength(FColumns[i].Data, 0);
  FRowCount := 0;
end;

function TDuckFrame.Tail(Count: Integer = 5): TDuckFrame;
var
  Col, Row, StartRow: Integer;
  RowsToCopy: Integer;
begin
  Result := TDuckFrame.Create;
  try
    if Count > FRowCount then
      RowsToCopy := FRowCount
    else
      RowsToCopy := Count;
      
    StartRow := FRowCount - RowsToCopy;
    if StartRow < 0 then
      StartRow := 0;
      
    Result.FRowCount := RowsToCopy;
    SetLength(Result.FColumns, Length(FColumns));
    
    for Col := 0 to Length(FColumns) - 1 do
    begin
      Result.FColumns[Col].Name := FColumns[Col].Name;
      Result.FColumns[Col].DataType := FColumns[Col].DataType;
      SetLength(Result.FColumns[Col].Data, RowsToCopy);
      
      for Row := 0 to RowsToCopy - 1 do
        Result.FColumns[Col].Data[Row] := FColumns[Col].Data[StartRow + Row];
    end;
  except
    Result.Free;
    raise;
  end;
end;

function TDuckFrame.Select(const ColumnNames: array of string): TDuckFrame;
var
  I, Col, NewCol: Integer;
  ColIndex: Integer;
begin
  Result := TDuckFrame.Create;
  try
    Result.FRowCount := FRowCount;
    SetLength(Result.FColumns, Length(ColumnNames));
    
    NewCol := 0;
    for I := 0 to Length(ColumnNames) - 1 do
    begin
      ColIndex := FindColumnIndex(ColumnNames[I]);
      if ColIndex = -1 then
        Continue;
        
      Result.FColumns[NewCol].Name := FColumns[ColIndex].Name;
      Result.FColumns[NewCol].DataType := FColumns[ColIndex].DataType;
      SetLength(Result.FColumns[NewCol].Data, FRowCount);
      
      for Col := 0 to FRowCount - 1 do
        Result.FColumns[NewCol].Data[Col] := FColumns[ColIndex].Data[Col];
        
      Inc(NewCol);
    end;
    
    if NewCol = 0 then
    begin
      Result.Free;
      raise EDuckDBError.Create('No valid columns selected');
    end;
    
    SetLength(Result.FColumns, NewCol);
  except
    Result.Free;
    raise;
  end;
end;

{ Maps DuckDB type to TDuckDBColumnType  }
function TDuckFrame.MapDuckDBType(duckdb_type: duckdb_type): TDuckDBColumnType;
begin
  case duckdb_type of
    DUCKDB_TYPE_INVALID: Result := dctUnknown;
    DUCKDB_TYPE_BOOLEAN: Result := dctBoolean;
    DUCKDB_TYPE_TINYINT: Result := dctTinyInt;
    DUCKDB_TYPE_SMALLINT: Result := dctSmallInt;
    DUCKDB_TYPE_INTEGER: Result := dctInteger;
    DUCKDB_TYPE_BIGINT: Result := dctBigInt;
    DUCKDB_TYPE_HUGEINT: Result := dctBigInt;
    DUCKDB_TYPE_UTINYINT: Result := dctTinyInt;
    DUCKDB_TYPE_USMALLINT: Result := dctSmallInt;
    DUCKDB_TYPE_UINTEGER: Result := dctInteger;
    DUCKDB_TYPE_UBIGINT: Result := dctBigInt;
    DUCKDB_TYPE_FLOAT: Result := dctFloat;
    DUCKDB_TYPE_DOUBLE: Result := dctDouble;
    DUCKDB_TYPE_TIMESTAMP: Result := dctTimestamp;
    DUCKDB_TYPE_DATE: Result := dctDate;
    DUCKDB_TYPE_TIME: Result := dctTime;
    DUCKDB_TYPE_INTERVAL: Result := dctString;
    DUCKDB_TYPE_VARCHAR: Result := dctString;
    DUCKDB_TYPE_BLOB: Result := dctBlob;
    else Result := dctUnknown;
  end;
end;

{ Loads data from a DuckDB result / Blank dataframe with DuckDB's datatypes }
procedure TDuckFrame.LoadFromResult(AResult: pduckdb_result);
var
  ColCount, Row, Col: Integer;
  StrValue: PAnsiChar;
  TempDateTime: TDateTime;  // Added for date/timestamp handling
  TempTime: TTime;         // Added for time handling
begin
  Clear;
  
  ColCount := duckdb_column_count(AResult);
  FRowCount := duckdb_row_count(AResult);
  
  SetLength(FColumns, ColCount);
  
  for Col := 0 to ColCount - 1 do
  begin
    // Set column metadata
    FColumns[Col].Name := string(AnsiString(duckdb_column_name(AResult, Col)));
    FColumns[Col].DataType := MapDuckDBType(duckdb_column_type(AResult, Col));
    
    // Allocate space for data
    SetLength(FColumns[Col].Data, FRowCount);
    
    // Load data
    for Row := 0 to FRowCount - 1 do
    begin
      if duckdb_value_is_null(AResult, Col, Row) then
      begin
        FColumns[Col].Data[Row] := Null;
        Continue;
      end;

      case FColumns[Col].DataType of
        dctBoolean:
          FColumns[Col].Data[Row] := duckdb_value_boolean(AResult, Col, Row);
        dctTinyInt:
          FColumns[Col].Data[Row] := duckdb_value_int8(AResult, Col, Row);
        dctSmallInt:
          FColumns[Col].Data[Row] := duckdb_value_int16(AResult, Col, Row);
        dctInteger:
          FColumns[Col].Data[Row] := duckdb_value_int32(AResult, Col, Row);
        dctBigInt:
          FColumns[Col].Data[Row] := duckdb_value_int64(AResult, Col, Row);
        dctFloat:
          FColumns[Col].Data[Row] := duckdb_value_float(AResult, Col, Row);
        dctDouble:
          FColumns[Col].Data[Row] := duckdb_value_double(AResult, Col, Row);
        dctDate:
        { Store date portion of TDateTime, which is an integer value }
          begin
            StrValue := duckdb_value_varchar(AResult, Col, Row);
            if StrValue <> nil then
            begin
              if TryStrToDateTime(string(AnsiString(StrValue)), TempDateTime) then
                FColumns[Col].Data[Row] := Int(TempDateTime)  // Store just the date portion
              else
                FColumns[Col].Data[Row] := Null;
              duckdb_free(StrValue);
            end
            else
              FColumns[Col].Data[Row] := Null;
          end;
        dctTime:
        { Store time portion of TDateTime, which is a fractional value }
          begin
            StrValue := duckdb_value_varchar(AResult, Col, Row);
            if StrValue <> nil then
            begin
              if TryStrToTime(string(AnsiString(StrValue)), TempTime) then
                FColumns[Col].Data[Row] := TempTime
              else
                FColumns[Col].Data[Row] := Null;
              duckdb_free(StrValue);
            end
            else
              FColumns[Col].Data[Row] := Null;
          end;
        dctTimestamp:
        { Store full datetime value, which is a Double where the integer part
          is the date and the fractional part is the time. }
          begin
            StrValue := duckdb_value_varchar(AResult, Col, Row);
            if StrValue <> nil then
            begin
              if TryStrToDateTime(string(AnsiString(StrValue)), TempDateTime) then
                FColumns[Col].Data[Row] := TempDateTime  // Store full datetime value
              else
                FColumns[Col].Data[Row] := Null;
              duckdb_free(StrValue);
            end
            else
              FColumns[Col].Data[Row] := Null;
          end;    
        dctString:
          begin
            StrValue := duckdb_value_varchar(AResult, Col, Row);
            if StrValue <> nil then
            begin
              FColumns[Col].Data[Row] := string(AnsiString(StrValue));
              duckdb_free(StrValue);
            end
            else
              FColumns[Col].Data[Row] := Null;
          end;
        else
          begin
            // Try to get as string for unhandled types
            StrValue := duckdb_value_varchar(AResult, Col, Row);
            if StrValue <> nil then
            begin
              FColumns[Col].Data[Row] := string(AnsiString(StrValue));
              duckdb_free(StrValue);
            end
            else
              FColumns[Col].Data[Row] := Null;
          end;
      end;
    end;
  end;
end;

procedure TDuckFrame.Print(MaxRows: Integer = 10);
var
  Col, Row, ColWidth: Integer;
  StartRow, EndRow: Integer;
  ColWidths: array of Integer;
  S: string;
begin
  if Length(FColumns) = 0 then
  begin
    WriteLn('Empty DataFrame');
    Exit;
  end;

  // Calculate column widths
  SetLength(ColWidths, Length(FColumns));
  for Col := 0 to Length(FColumns) - 1 do
  begin
    ColWidths[Col] := Length(FColumns[Col].Name);
    for Row := 0 to FRowCount - 1 do
    begin
      // Format date/time values properly for width calculation
      if VarIsNull(FColumns[Col].Data[Row]) then
        S := ''
      else
        case FColumns[Col].DataType of
          dctDate:
            S := FormatDateTime('dd/mm/yyyy', VarToDateTime(FColumns[Col].Data[Row]));
          dctTime:
            S := FormatDateTime('hh:nn:ss', VarToDateTime(FColumns[Col].Data[Row]));
          dctTimestamp:
            S := FormatDateTime('dd/mm/yyyy hh:nn:ss', VarToDateTime(FColumns[Col].Data[Row]));
          else
            S := VarToStr(FColumns[Col].Data[Row]);
        end;
      
      if Length(S) > ColWidths[Col] then
        ColWidths[Col] := Length(S);
    end;
  end;

  // Print header
  for Col := 0 to Length(FColumns) - 1 do
    Write(Format('%-*s ', [ColWidths[Col] + 1, FColumns[Col].Name]));
  WriteLn;

  // Print separator
  for Col := 0 to Length(FColumns) - 1 do
  begin
    for ColWidth := 1 to ColWidths[Col] do
      Write('-');
    Write(' ');
  end;
  WriteLn;

  // Determine rows to print
  if (MaxRows > 0) and (FRowCount > MaxRows) then
  begin
    StartRow := 0;
    EndRow := MaxRows div 2;
    
    // Print first half
    for Row := StartRow to EndRow - 1 do
    begin
      for Col := 0 to Length(FColumns) - 1 do
      begin
        if VarIsNull(FColumns[Col].Data[Row]) then
          S := ''
        else
          case FColumns[Col].DataType of
            dctDate:
              S := FormatDateTime('dd/mm/yyyy', VarToDateTime(FColumns[Col].Data[Row]));
            dctTime:
              S := FormatDateTime('hh:nn:ss', VarToDateTime(FColumns[Col].Data[Row]));
            dctTimestamp:
              S := FormatDateTime('dd/mm/yyyy hh:nn:ss', VarToDateTime(FColumns[Col].Data[Row]));
            else
              S := VarToStr(FColumns[Col].Data[Row]);
          end;
        Write(Format('%-*s ', [ColWidths[Col] + 1, S]));
      end;
      WriteLn;
    end;
    
    WriteLn('...');
    
    // Print last half
    StartRow := FRowCount - (MaxRows div 2);
    EndRow := FRowCount - 1;
    for Row := StartRow to EndRow do
    begin
      for Col := 0 to Length(FColumns) - 1 do
      begin
        if VarIsNull(FColumns[Col].Data[Row]) then
          S := ''
        else
          case FColumns[Col].DataType of
            dctDate:
              S := FormatDateTime('dd/mm/yyyy', VarToDateTime(FColumns[Col].Data[Row]));
            dctTime:
              S := FormatDateTime('hh:nn:ss', VarToDateTime(FColumns[Col].Data[Row]));
            dctTimestamp:
              S := FormatDateTime('dd/mm/yyyy hh:nn:ss', VarToDateTime(FColumns[Col].Data[Row]));
            else
              S := VarToStr(FColumns[Col].Data[Row]);
          end;
        Write(Format('%-*s ', [ColWidths[Col] + 1, S]));
      end;
      WriteLn;
    end;
    
    WriteLn(Format('[%d rows x %d columns]', [FRowCount, Length(FColumns)]));
  end
  else
  begin
    // Print all rows
    for Row := 0 to FRowCount - 1 do
    begin
      for Col := 0 to Length(FColumns) - 1 do
      begin
        if VarIsNull(FColumns[Col].Data[Row]) then
          S := ''
        else
          case FColumns[Col].DataType of
            dctDate:
              S := FormatDateTime('dd/mm/yyyy', VarToDateTime(FColumns[Col].Data[Row]));
            dctTime:
              S := FormatDateTime('hh:nn:ss', VarToDateTime(FColumns[Col].Data[Row]));
            dctTimestamp:
              S := FormatDateTime('dd/mm/yyyy hh:nn:ss', VarToDateTime(FColumns[Col].Data[Row]));
            else
              S := VarToStr(FColumns[Col].Data[Row]);
          end;
        Write(Format('%-*s ', [ColWidths[Col] + 1, S]));
      end;
      WriteLn;
    end;
  end;
end;

function TDuckFrame.Head(Count: Integer = 5): TDuckFrame;
var
  Col, Row: Integer;
  RowsToCopy: Integer;
begin
  Result := TDuckFrame.Create;
  try
    RowsToCopy := Count;
    if RowsToCopy > FRowCount then
      RowsToCopy := FRowCount;
      
    Result.FRowCount := RowsToCopy;
    SetLength(Result.FColumns, Length(FColumns));
    
    for Col := 0 to Length(FColumns) - 1 do
    begin
      Result.FColumns[Col].Name := FColumns[Col].Name;
      Result.FColumns[Col].DataType := FColumns[Col].DataType;
      SetLength(Result.FColumns[Col].Data, RowsToCopy);
      
      for Row := 0 to RowsToCopy - 1 do
        Result.FColumns[Col].Data[Row] := FColumns[Col].Data[Row];
    end;
  except
    Result.Free;
    raise;
  end;
end;

procedure TDuckFrame.SaveToCSV(const FileName: string);
const
  CRLF = #13#10;  // RFC 4180 specifies CRLF
var
  F: TextFile;
  Row, Col: Integer;
  Value: string;

  // Helper function to properly escape and quote CSV fields per RFC 4180
  function EscapeCSVField(const Field: string): string;
  var
    NeedsQuoting: Boolean;
  begin
    Result := Field;
    
    // Check if field needs quoting
    NeedsQuoting := (Pos('"', Result) > 0) or 
                    (Pos(',', Result) > 0) or 
                    (Pos(#13, Result) > 0) or 
                    (Pos(#10, Result) > 0);
                    
    // Escape double quotes with double quotes (RFC 4180 rule 7)
    Result := StringReplace(Result, '"', '""', [rfReplaceAll]);
    
    // Enclose in quotes if needed (RFC 4180 rule 6)
    if NeedsQuoting then
      Result := '"' + Result + '"';
  end;

begin
  AssignFile(F, FileName);
  try
    Rewrite(F);
    
    // Write header (RFC 4180 rule 3)
    for Col := 0 to Length(FColumns) - 1 do
    begin
      if Col > 0 then
        Write(F, ',');
      Write(F, EscapeCSVField(FColumns[Col].Name));
    end;
    Write(F, CRLF);  // Use CRLF (RFC 4180 rule 1)
    
    // Write data rows
    for Row := 0 to FRowCount - 1 do
    begin
      for Col := 0 to Length(FColumns) - 1 do
      begin
        // Add comma between fields (RFC 4180 rule 5)
        if Col > 0 then
          Write(F, ',');
          
        if VarIsNull(FColumns[Col].Data[Row]) then
          // Empty field for NULL (RFC 4180 rule 9)
          Write(F, '')
        else
          Write(F, EscapeCSVField(VarToStr(FColumns[Col].Data[Row])));
      end;
      
      // Add CRLF after each record except possibly the last (RFC 4180 rules 1,2)
      if Row < FRowCount - 1 then
        Write(F, CRLF);
    end;
  finally
    CloseFile(F);
  end;
end;

function TDuckFrame.IsNumericColumn(const Col: TDuckDBColumn): Boolean;
begin
  Result := Col.DataType in [dctTinyInt, dctSmallInt, dctInteger, dctBigInt, 
                            dctFloat, dctDouble];
end;

function TDuckFrame.CalculateColumnStats(const Col: TDuckDBColumn): TColumnStats;
type
  TValueCount = record
    Value: string;
    Count: Integer;
  end;
var
  NumericValues: array of Double;
  ValidCount, I: Integer;
  Value: Variant;
  SumDiff3, SumDiff4: Double;
  Diff: Double;
  StdDevCubed, StdDevFourth: Double;
  N: Double;
  // For categorical variables
  FreqMap: specialize TDictionary<string, Integer>;
  TopValues: array of TValueCount;
  StrValue: string;
begin
  Result := Default(TColumnStats);
  
  if IsNumericColumn(Col) then
  begin
    // Initialize arrays for numeric calculations
    SetLength(NumericValues, FRowCount);
    ValidCount := 0;
    Result.NullCount := 0;
    
    // Collect valid numeric values
    for I := 0 to FRowCount - 1 do
    begin
      if VarIsNull(Col.Data[I]) then
        Inc(Result.NullCount)
      else
      begin
        NumericValues[ValidCount] := VarAsType(Col.Data[I], varDouble);
        Inc(ValidCount);
      end;
    end;
    
    // Resize array to actual valid count
    SetLength(NumericValues, ValidCount);
    
    if ValidCount > 0 then
    begin
      // Sort values for percentile calculations
      QuickSort(NumericValues, 0, ValidCount - 1);
      
      // Calculate basic stats
      Result.Count := FRowCount;
      Result.NonMissingRate := ValidCount / FRowCount;
      Result.Min := NumericValues[0];
      Result.Max := NumericValues[ValidCount - 1];
      Result.Q1 := CalculatePercentile(NumericValues, 0.25);
      Result.Median := CalculatePercentile(NumericValues, 0.50);
      Result.Q3 := CalculatePercentile(NumericValues, 0.75);
      
      // Calculate mean
      Result.Mean := 0;
      for I := 0 to ValidCount - 1 do
        Result.Mean := Result.Mean + NumericValues[I];
      Result.Mean := Result.Mean / ValidCount;
      
      // Calculate standard deviation
      Result.StdDev := 0;
      for I := 0 to ValidCount - 1 do
        Result.StdDev := Result.StdDev + Sqr(NumericValues[I] - Result.Mean);
      if ValidCount > 1 then
        Result.StdDev := Sqrt(Result.StdDev / (ValidCount - 1))
      else
        Result.StdDev := 0;
        
      // Calculate skewness and kurtosis
      if (ValidCount > 2) and (Result.StdDev > 0) then
      begin
        SumDiff3 := 0;
        SumDiff4 := 0;
        N := ValidCount;
        
        for I := 0 to ValidCount - 1 do
        begin
          Diff := NumericValues[I] - Result.Mean;
          SumDiff3 := SumDiff3 + Power(Diff, 3);
          SumDiff4 := SumDiff4 + Power(Diff, 4);
        end;
        
        // Sample Skewness
        if ValidCount > 2 then
          Result.Skewness := (Sqrt(N) * (N-1) / (N-2)) * 
                            (SumDiff3 / (N * Power(Result.StdDev, 3)))
        else
          Result.Skewness := 0;
        
        // Sample Kurtosis
        if ValidCount > 3 then
          Result.Kurtosis := (N*(N+1)*(N-1) / ((N-2)*(N-3))) * 
                            (SumDiff4 / (N * Power(Result.StdDev, 4))) -
                            (3 * Sqr(N-1) / ((N-2)*(N-3)))
        else
          Result.Kurtosis := 0;
      end;
    end;
  end
  else if Col.DataType = dctString then
  begin
    FreqMap := specialize TDictionary<string, Integer>.Create;
    try
      // Count frequencies
      Result.NullCount := 0;
      for I := 0 to FRowCount - 1 do
      begin
        Value := Col.Data[I];
        if VarIsNull(Value) then
          Inc(Result.NullCount)
        else
        begin
          StrValue := VarToStr(Value);
          if FreqMap.ContainsKey(StrValue) then
            FreqMap[StrValue] := FreqMap[StrValue] + 1
          else
            FreqMap.Add(StrValue, 1);
        end;
      end;

      // Calculate categorical stats
      Result.Count := FRowCount;
      Result.NUnique := FreqMap.Count;
      Result.NonMissingRate := (FRowCount - Result.NullCount) / FRowCount;
      Result.Ordered := False;  // We don't check for ordering yet
      
      // Get top counts
      SetLength(TopValues, FreqMap.Count);
      I := 0;
      for StrValue in FreqMap.Keys do
      begin
        TopValues[I].Value := StrValue;
        TopValues[I].Count := FreqMap[StrValue];
        Inc(I);
      end;
      
      // Format top counts string
      Result.TopCounts := '';
      for I := 0 to Min(2, High(TopValues)) do
      begin
        if I > 0 then
          Result.TopCounts := Result.TopCounts + ', ';
        Result.TopCounts := Result.TopCounts + 
          Format('%s: %d', [TopValues[I].Value, TopValues[I].Count]);
      end;
    finally
      FreqMap.Free;
    end;
  end;
end;

function TDuckFrame.CalculatePercentile(const Values: array of Double; Percentile: Double): Double;
var
  N: Integer;
  Position: Double;
  Lower: Integer;
  Delta: Double;
  ClampedPercentile: Double;
begin
  // Clamp percentile value to [0,1] range
  ClampedPercentile := Percentile;
  if ClampedPercentile < 0 then
    ClampedPercentile := 0
  else if ClampedPercentile > 1 then
    ClampedPercentile := 1;

  // Handle empty or single-value arrays
  N := Length(Values);
  if N = 0 then
    Exit(0);
  if N = 1 then
    Exit(Values[0]);

  // Calculate interpolation position
  Position := ClampedPercentile * (N - 1);
  Lower := Trunc(Position);
  Delta := Position - Lower;
  
  // Handle edge case and interpolate
  if Lower + 1 >= N then
    Result := Values[N - 1]
  else
    Result := Values[Lower] + Delta * (Values[Lower + 1] - Values[Lower]);
end;

procedure TDuckFrame.Describe;
var
  Col: Integer;
  Stats: TColumnStats;
  NumericCols, CategoricalCols: array of Integer;
  I: Integer;
  MinVal, MaxVal: Double;
begin
  if Length(FColumns) = 0 then
  begin
    WriteLn('Empty DataFrame');
    Exit;
  end;

  // Separate columns by type
  SetLength(NumericCols, 0);
  SetLength(CategoricalCols, 0);
  for I := 0 to High(FColumns) do
  begin
    if IsNumericColumn(FColumns[I]) then
    begin
      SetLength(NumericCols, Length(NumericCols) + 1);
      NumericCols[High(NumericCols)] := I;
    end
    else if FColumns[I].DataType = dctString then
    begin
      SetLength(CategoricalCols, Length(CategoricalCols) + 1);
      CategoricalCols[High(CategoricalCols)] := I;
    end;
  end;

  // Print DataFrame overview

  WriteLn('Number of rows: ', FRowCount);
  WriteLn('Number of columns: ', Length(FColumns));
  WriteLn;
  
  // Print column type frequency
  WriteLn('Column type frequency:');
  WriteLn('  factor    ', Length(CategoricalCols));
  WriteLn('  numeric   ', Length(NumericCols));
  WriteLn;

  // Print categorical variables
  if Length(CategoricalCols) > 0 then
  begin
    WriteLn('-- Variable type: factor');
    WriteLn(Format('%-16s %-10s %-13s %-9s %-8s',
      ['skim_variable', 'n_missing', 'complete_rate', 'ordered', 'n_unique']));
    
    for I := 0 to High(CategoricalCols) do
    begin
      Col := CategoricalCols[I];
      Stats := CalculateColumnStats(FColumns[Col]);
      
      WriteLn(Format('%-16s %-10d %-13.3f %-9s %-8d',
        [FColumns[Col].Name,
         Stats.NullCount,
         Stats.NonMissingRate,
         BoolToStr(Stats.Ordered, 'TRUE', 'FALSE'),
         Stats.NUnique]));
         
      if Stats.TopCounts <> '' then
        WriteLn('    Top counts: ', Stats.TopCounts);
    end;
    WriteLn;
  end;

if Length(NumericCols) > 0 then
begin
  WriteLn('-- Variable type: numeric');
  WriteLn(Format('%-16s %-10s %-13s %-9s %-9s %-9s %-9s %-9s %-9s %-9s %-9s %-9s',
    ['skim_variable', 'n_missing', 'complete_rate', 'mean', 'sd', 'min', 'q1', 'median', 'q3', 'max', 'skew', 'kurt']));
  
  for I := 0 to High(NumericCols) do
  begin
    Col := NumericCols[I];
    Stats := CalculateColumnStats(FColumns[Col]);
    
    if VarIsNull(Stats.Min) then
      MinVal := 0
    else
      MinVal := VarAsType(Stats.Min, varDouble);
      
    if VarIsNull(Stats.Max) then
      MaxVal := 0
    else
      MaxVal := VarAsType(Stats.Max, varDouble);
    
    WriteLn(Format('%-16s %-10d %-13.3f %-9.3f %-9.3f %-9.3f %-9.3f %-9.3f %-9.3f %-9.3f %-9.3f %-9.3f',
      [FColumns[Col].Name,
       Stats.NullCount,
       Stats.NonMissingRate,
       Stats.Mean,
       Stats.StdDev,
       MinVal,
       Stats.Q1,
       Stats.Median,
       Stats.Q3,
       MaxVal,
       Stats.Skewness,
       Stats.Kurtosis]));
  end;
end;
end;

function TDuckFrame.NullCount: TDuckFrame;
var
  Col: Integer;
  Row: Integer;
  NullCounts: array of Integer;
begin
  Result := TDuckFrame.Create;
  try
    SetLength(NullCounts, Length(FColumns));
    
    // Calculate null counts for each column
    for Col := 0 to Length(FColumns) - 1 do
    begin
      NullCounts[Col] := 0;
      for Row := 0 to FRowCount - 1 do
        if VarIsNull(FColumns[Col].Data[Row]) then
          Inc(NullCounts[Col]);
    end;

    // Create result DataFrame with single row
    Result.FRowCount := 1;
    SetLength(Result.FColumns, Length(FColumns));
    
    for Col := 0 to Length(FColumns) - 1 do
    begin
      Result.FColumns[Col].Name := FColumns[Col].Name;
      Result.FColumns[Col].DataType := dctInteger;
      SetLength(Result.FColumns[Col].Data, 1);
      Result.FColumns[Col].Data[0] := NullCounts[Col];  // Store as integer
    end;
  except
    Result.Free;
    raise;
  end;
end;

procedure TDuckFrame.Info;
var
  Col: Integer;
  TotalMemory: Int64;
  NullsInColumn: Integer;
  Row: Integer;
begin
  WriteLn(Format('DataFrame: %d rows × %d columns', [FRowCount, Length(FColumns)]));
  WriteLn;
  
  WriteLn('Columns:');
  for Col := 0 to Length(FColumns) - 1 do
  begin
    // Count nulls manually
    NullsInColumn := 0;
    for Row := 0 to FRowCount - 1 do
      if VarIsNull(FColumns[Col].Data[Row]) then
        Inc(NullsInColumn);
        
    WriteLn(Format('  %s: %s (nulls: %d)',
      [FColumns[Col].Name, 
       GetEnumName(TypeInfo(TDuckDBColumnType), Ord(FColumns[Col].DataType)), 
       NullsInColumn]));
  end;
  
  // Estimate memory usage (rough calculation)
  TotalMemory := 0;
  for Col := 0 to Length(FColumns) - 1 do
    TotalMemory := TotalMemory + (FRowCount * SizeOf(Variant));
    
  WriteLn;
  WriteLn(Format('Memory usage: %d bytes (%.2f MB)', 
                 [TotalMemory, TotalMemory / (1024 * 1024)]));
end;

{
  QuickSort for numeric columns
  241125
  - Added ascending/descending parameter
  - Added null value handling
  - Added bounds checking
  - Consistent with QuickSortWithIndices null handling
Early exit for single-element or empty ranges
}
procedure QuickSort(var A: array of Double; iLo, iHi: Integer; Ascending: Boolean = True);
var
  Lo, Hi: Integer;
  Pivot, T: Double;
  PivotIsNull: Boolean;
begin
  if iLo >= iHi then Exit;
  
  Lo := iLo;
  Hi := iHi;
  Pivot := A[(Lo + Hi) div 2];
  PivotIsNull := VarIsNull(Pivot);

  repeat
    if Ascending then
    begin
      while (Lo <= iHi) and not VarIsNull(A[Lo]) and (A[Lo] < Pivot) do Inc(Lo);
      while (Hi >= iLo) and (VarIsNull(A[Hi]) or (A[Hi] > Pivot)) do Dec(Hi);
    end
    else
    begin
      while (Lo <= iHi) and (VarIsNull(A[Lo]) or (A[Lo] > Pivot)) do Inc(Lo);
      while (Hi >= iLo) and not (VarIsNull(A[Hi]) and (A[Hi] < Pivot)) do Dec(Hi);
    end;

    if Lo <= Hi then
    begin
      T := A[Lo];
      A[Lo] := A[Hi];
      A[Hi] := T;
      Inc(Lo);
      Dec(Hi);
    end;
  until Lo > Hi;

  if Hi > iLo then QuickSort(A, iLo, Hi, Ascending);
  if Lo < iHi then QuickSort(A, Lo, iHi, Ascending);
end;

procedure QuickSortWithIndices(var Values: array of Double; var Indices: array of Integer; 
  Left, Right: Integer; Ascending: Boolean = True);
var
  I, J: Integer;
  Pivot, TempValue: Double;
  TempIndex: Integer;
  PivotIsNull: Boolean;
begin
  if Left < Right then
  begin
    I := Left;
    J := Right;
    
    // Choose middle element as pivot
    Pivot := Values[(Left + Right) div 2];
    PivotIsNull := VarIsNull(Pivot);
    
    repeat
      // For ascending: nulls go to end, so they're "greater" than non-nulls
      // For descending: nulls go to start, so they're "less" than non-nulls
      if Ascending then
      begin
        while (I <= Right) and 
              ((VarIsNull(Values[I]) and not PivotIsNull) or 
               (not VarIsNull(Values[I]) and not PivotIsNull and (Values[I] < Pivot))) do Inc(I);
        while (J >= Left) and 
              ((not VarIsNull(Values[J]) and PivotIsNull) or 
               (not VarIsNull(Values[J]) and not PivotIsNull and (Values[J] > Pivot))) do Dec(J);
      end
      else
      begin
        while (I <= Right) and 
              ((not VarIsNull(Values[I]) and PivotIsNull) or 
               (not VarIsNull(Values[I]) and not PivotIsNull and (Values[I] > Pivot))) do Inc(I);
        while (J >= Left) and 
              ((VarIsNull(Values[J]) and not PivotIsNull) or 
               (not VarIsNull(Values[J]) and not PivotIsNull and (Values[J] < Pivot))) do Dec(J);
      end;
      
      if I <= J then
      begin
        // Swap values
        TempValue := Values[I];
        Values[I] := Values[J];
        Values[J] := TempValue;
        
        // Swap indices
        TempIndex := Indices[I];
        Indices[I] := Indices[J];
        Indices[J] := TempIndex;
        
        Inc(I);
        Dec(J);
      end;
    until I > J;
    
    // Recursively sort both partitions
    if Left < J then
      QuickSortWithIndices(Values, Indices, Left, J, Ascending);
    if I < Right then
      QuickSortWithIndices(Values, Indices, I, Right, Ascending);
  end;
end;


procedure QuickSortWithStrings(var Values: array of string; var Indices: array of Integer;
  Left, Right: Integer; Ascending: Boolean = True);
var
  I, J: Integer;
  Pivot, TempValue: string;
  TempIndex: Integer;
begin
  if Left < Right then
  begin
    I := Left;
    J := Right;
    Pivot := Values[(Left + Right) div 2];
    
    repeat
      if Ascending then
      begin
        while Values[I] < Pivot do Inc(I);
        while Values[J] > Pivot do Dec(J);
      end
      else
      begin
        while Values[I] > Pivot do Inc(I);
        while Values[J] < Pivot do Dec(J);
      end;
      
      if I <= J then
      begin
        // Swap values
        TempValue := Values[I];
        Values[I] := Values[J];
        Values[J] := TempValue;
        
        // Swap indices
        TempIndex := Indices[I];
        Indices[I] := Indices[J];
        Indices[J] := TempIndex;
        
        Inc(I);
        Dec(J);
      end;
    until I > J;
    
    if Left < J then
      QuickSortWithStrings(Values, Indices, Left, J, Ascending);
    if I < Right then
      QuickSortWithStrings(Values, Indices, I, Right, Ascending);
  end;
end;

// Correlation matrix
function TDuckFrame.CorrPearson: TDuckFrame;
var
  NumericCols: array of Integer;
  I, J, K: Integer;
  ColI, ColJ: TDuckDBColumn;
  MeanI, MeanJ, SumI, SumJ: Double;
  CovIJ, StdDevI, StdDevJ: Double;
  ValidCount: Integer;
  VI, VJ: Double;
begin
  Result := TDuckFrame.Create;
  try
    // Find numeric columns
    SetLength(NumericCols, 0);
    for I := 0 to High(FColumns) do
      if IsNumericColumn(FColumns[I]) then
      begin
        SetLength(NumericCols, Length(NumericCols) + 1);
        NumericCols[High(NumericCols)] := I;
      end;
      
    // Create correlation matrix
    Result.FRowCount := Length(NumericCols);
    SetLength(Result.FColumns, Length(NumericCols));
    
    // Setup column names and types
    for I := 0 to High(NumericCols) do
    begin
      Result.FColumns[I].Name := FColumns[NumericCols[I]].Name;
      Result.FColumns[I].DataType := dctDouble;
      SetLength(Result.FColumns[I].Data, Result.FRowCount);
    end;
    
    // Calculate correlations
    for I := 0 to High(NumericCols) do
    begin
      ColI := FColumns[NumericCols[I]];
      
      for J := 0 to High(NumericCols) do
      begin
        ColJ := FColumns[NumericCols[J]];
        
        // Initialize
        SumI := 0;
        SumJ := 0;
        ValidCount := 0;
        
        // Calculate means
        for K := 0 to FRowCount - 1 do
        begin
          if not (VarIsNull(ColI.Data[K]) or VarIsNull(ColJ.Data[K])) then
          begin
            SumI := SumI + ColI.Data[K];
            SumJ := SumJ + ColJ.Data[K];
            Inc(ValidCount);
          end;
        end;
        
        if ValidCount < 2 then
        begin
          Result.FColumns[I].Data[J] := Null;
          Continue;
        end;
        
        MeanI := SumI / ValidCount;
        MeanJ := SumJ / ValidCount;
        
        // Calculate covariance and standard deviations
        CovIJ := 0;
        StdDevI := 0;
        StdDevJ := 0;
        
        for K := 0 to FRowCount - 1 do
        begin
          if not (VarIsNull(ColI.Data[K]) or VarIsNull(ColJ.Data[K])) then
          begin
            VI := ColI.Data[K] - MeanI;
            VJ := ColJ.Data[K] - MeanJ;
            CovIJ := CovIJ + VI * VJ;
            StdDevI := StdDevI + VI * VI;
            StdDevJ := StdDevJ + VJ * VJ;
          end;
        end;
        
        StdDevI := Sqrt(StdDevI / (ValidCount - 1));
        StdDevJ := Sqrt(StdDevJ / (ValidCount - 1));
        
        // Calculate correlation
        if (StdDevI > 0) and (StdDevJ > 0) then
          Result.FColumns[I].Data[J] := CovIJ / ((ValidCount - 1) * StdDevI * StdDevJ)
        else
          Result.FColumns[I].Data[J] := Null;
      end;
    end;
  except
    Result.Free;
    raise;
  end;
end;


// Basic plotting capabilities
procedure TDuckFrame.PlotHistogram(const ColumnName: string; Bins: Integer = 10);
const
  MAX_WIDTH = 50;    // Maximum width of the histogram in characters
  BAR_CHAR = '#';    // Character to use for the bars
  MIN_BAR = 3;       // Minimum bar width for non-zero counts
  MAX_BAR = 10;      // Reduced maximum bar width
var
  Col: TDuckDBColumn;
  MinVal, MaxVal, BinWidth: Double;
  BinCounts: array of Integer;
  DataPoints: array of Double;
  ValidCount, MaxCount: Integer;
  I, BinIndex: Integer;
  Scale: Double;
  BarWidth: Integer;
  BinStart, BinEnd: Double;
  BinLabel: string;
begin
  // Get column and validate
  Col := GetColumnByName(ColumnName);
  if not IsNumericColumn(Col) then
    raise EDuckDBError.Create('Histogram requires a numeric column');

  // Collect valid values and find min/max
  SetLength(DataPoints, FRowCount);
  ValidCount := 0;
  MinVal := 0;
  MaxVal := 0;
  
  for I := 0 to FRowCount - 1 do
  begin
    if not VarIsNull(Col.Data[I]) then
    begin
      DataPoints[ValidCount] := Col.Data[I];
      if ValidCount = 0 then
      begin
        MinVal := DataPoints[ValidCount];
        MaxVal := DataPoints[ValidCount];
      end
      else
      begin
        if DataPoints[ValidCount] < MinVal then MinVal := DataPoints[ValidCount];
        if DataPoints[ValidCount] > MaxVal then MaxVal := DataPoints[ValidCount];
      end;
      Inc(ValidCount);
    end;
  end;

  // Initialize bin counts
  SetLength(BinCounts, Bins);
  BinWidth := (MaxVal - MinVal) / Bins;
  
  // Count values in each bin
  MaxCount := 0;
  for I := 0 to ValidCount - 1 do
  begin
    BinIndex := Trunc((DataPoints[I] - MinVal) / BinWidth);
    if BinIndex = Bins then  // Handle edge case for maximum value
      BinIndex := Bins - 1;
    Inc(BinCounts[BinIndex]);
    if BinCounts[BinIndex] > MaxCount then
      MaxCount := BinCounts[BinIndex];
  end;

  // Calculate scale factor for display
  if MaxCount > 1 then
    Scale := (MAX_BAR - MIN_BAR) / (MaxCount - 1)
  else
    Scale := 0;  // For uniform counts of 1, we'll use MIN_BAR

  // Print histogram header
  WriteLn;
  WriteLn('Histogram of ', ColumnName);
  WriteLn('Range: ', MinVal:0:2, ' to ', MaxVal:0:2);
  WriteLn('Bin width: ', BinWidth:0:2);
  WriteLn('Total count: ', ValidCount);
  WriteLn;

  // Print each bin
  for I := 0 to Bins - 1 do
  begin
    BinStart := MinVal + (I * BinWidth);
    BinEnd := MinVal + ((I + 1) * BinWidth);
    
    if I = Bins - 1 then
      BinLabel := Format('[%.2f-%.2f]', [BinStart, BinEnd])
    else
      BinLabel := Format('[%.2f-%.2f)', [BinStart, BinEnd]);
    
    // For counts of 1, use MIN_BAR, otherwise calculate proportionally
    if BinCounts[I] > 0 then
      if MaxCount = 1 then
        BarWidth := MIN_BAR
      else
        BarWidth := MIN_BAR + Round((BinCounts[I] - 1) * Scale)
    else
      BarWidth := 0;
    
    // Print bar with right-aligned count
    Write(Format('%-15s |', [BinLabel]));
    Write(StringOfChar(BAR_CHAR, BarWidth));
    WriteLn(Format(' %3d', [BinCounts[I]]));
  end;
  WriteLn;
end;

// Missing data handling
function TDuckFrame.DropNA: TDuckFrame;  // Remove rows with any null values
var
  Col, Row: Integer;
  KeepRow: array of Boolean;
  NewRowCount: Integer;
  OldRowIndex: Integer;
begin
  Result := TDuckFrame.Create;
  try
    // Initialize array to track which rows to keep
    SetLength(KeepRow, FRowCount);
    NewRowCount := 0;
    
    // Mark rows that have no null values
    for Row := 0 to FRowCount - 1 do
    begin
      KeepRow[Row] := True;
      for Col := 0 to High(FColumns) do
      begin
        if VarIsNull(FColumns[Col].Data[Row]) then
        begin
          KeepRow[Row] := False;
          Break;
        end;
      end;
      if KeepRow[Row] then
        Inc(NewRowCount);
    end;
    
    // Create new DataFrame with same structure
    Result.FRowCount := NewRowCount;
    SetLength(Result.FColumns, Length(FColumns));
    
    // Copy column metadata and allocate space
    for Col := 0 to High(FColumns) do
    begin
      Result.FColumns[Col].Name := FColumns[Col].Name;
      Result.FColumns[Col].DataType := FColumns[Col].DataType;
      SetLength(Result.FColumns[Col].Data, NewRowCount);
    end;
    
    // Copy non-null rows
    OldRowIndex := 0;
    for Row := 0 to FRowCount - 1 do
    begin
      if KeepRow[Row] then
      begin
        for Col := 0 to High(FColumns) do
          Result.FColumns[Col].Data[OldRowIndex] := FColumns[Col].Data[Row];
        Inc(OldRowIndex);
      end;
    end;
  except
    Result.Free;
    raise;
  end;
end;

function TDuckFrame.FillNA(const Value: Variant): TDuckFrame;  // Fill null values
var
  Col, Row: Integer;
begin
  Result := TDuckFrame.Create;
  try
    // Copy structure
    Result.FRowCount := FRowCount;
    SetLength(Result.FColumns, Length(FColumns));
    
    // Copy data and fill nulls
    for Col := 0 to High(FColumns) do
    begin
      Result.FColumns[Col].Name := FColumns[Col].Name;
      Result.FColumns[Col].DataType := FColumns[Col].DataType;
      SetLength(Result.FColumns[Col].Data, FRowCount);
      
      for Row := 0 to FRowCount - 1 do
      begin
        if VarIsNull(FColumns[Col].Data[Row]) then
          Result.FColumns[Col].Data[Row] := Value
        else
          Result.FColumns[Col].Data[Row] := FColumns[Col].Data[Row];
      end;
    end;
  except
    Result.Free;
    raise;
  end;
end;

// Unique counts (frequency of each unique value)
function TDuckFrame.UniqueCounts(const ColumnName: string): TDuckFrame;
var
  Col: TDuckDBColumn;
  FreqMap: specialize TDictionary<string, Integer>;
  Value: Variant;
  StrValue: string;
  I: Integer;
  CurrentIndex: Integer;
begin
  Result := TDuckFrame.Create;
  FreqMap := specialize TDictionary<string, Integer>.Create;
  
  try
    // Find the column
    Col := GetColumnByName(ColumnName);
    
    // Count occurrences of each value
    for I := 0 to FRowCount - 1 do
    begin
      Value := Col.Data[I];
      if not VarIsNull(Value) then
      begin
        StrValue := VarToStr(Value);
        if FreqMap.ContainsKey(StrValue) then
          FreqMap[StrValue] := FreqMap[StrValue] + 1
        else
          FreqMap.Add(StrValue, 1);
      end;
    end;
    
    // Create result DataFrame
    SetLength(Result.FColumns, 2);
    Result.FColumns[0].Name := 'Value';
    Result.FColumns[0].DataType := Col.DataType;
    Result.FColumns[1].Name := 'Count';
    Result.FColumns[1].DataType := dctInteger;
    
    // Fill result data
    Result.FRowCount := FreqMap.Count;
    SetLength(Result.FColumns[0].Data, Result.FRowCount);
    SetLength(Result.FColumns[1].Data, Result.FRowCount);
    
    // Fill data directly from dictionary
    CurrentIndex := 0;
    for StrValue in FreqMap.Keys do
    begin
      Result.FColumns[0].Data[CurrentIndex] := VarAsType(StrValue, VarType(Col.Data[0]));
      Result.FColumns[1].Data[CurrentIndex] := FreqMap[StrValue];
      Inc(CurrentIndex);
    end;
  finally
    FreqMap.Free;
  end;
end;

// Add this new function to calculate Spearman correlation
function TDuckFrame.CorrSpearman: TDuckFrame;
var
  NumericCols: array of Integer;
  I, J, K, L: Integer;
  ColI, ColJ: TDuckDBColumn;
  ValidCount: Integer;
  // Arrays for sorting and ranking
  ValuesI, ValuesJ: array of Double;
  IndicesI, IndicesJ: array of Integer;
  RanksI, RanksJ: array of Double;
  SumDiffSq: Double;
  TempRank: Double;
  RankCount: Integer;
begin
  Result := TDuckFrame.Create;
  try
    // Find numeric columns
    SetLength(NumericCols, 0);
    for I := 0 to High(FColumns) do
      if IsNumericColumn(FColumns[I]) then
      begin
        SetLength(NumericCols, Length(NumericCols) + 1);
        NumericCols[High(NumericCols)] := I;
      end;
      
    // Create correlation matrix
    Result.FRowCount := Length(NumericCols);
    SetLength(Result.FColumns, Length(NumericCols));
    
    // Setup column names and types
    for I := 0 to High(NumericCols) do
    begin
      Result.FColumns[I].Name := FColumns[NumericCols[I]].Name;
      Result.FColumns[I].DataType := dctDouble;
      SetLength(Result.FColumns[I].Data, Result.FRowCount);
    end;
    
    // Calculate Spearman correlations
    for I := 0 to High(NumericCols) do
    begin
      ColI := FColumns[NumericCols[I]];
      
      for J := 0 to High(NumericCols) do
      begin
        ColJ := FColumns[NumericCols[J]];
        
        // Initialize
        ValidCount := 0;
        
        // Count valid pairs and collect data
        for K := 0 to FRowCount - 1 do
          if not (VarIsNull(ColI.Data[K]) or VarIsNull(ColJ.Data[K])) then
            Inc(ValidCount);
            
        if ValidCount < 2 then
        begin
          Result.FColumns[I].Data[J] := Null;
          Continue;
        end;
        
        // Initialize arrays
        SetLength(ValuesI, ValidCount);
        SetLength(ValuesJ, ValidCount);
        SetLength(IndicesI, ValidCount);
        SetLength(IndicesJ, ValidCount);
        SetLength(RanksI, ValidCount);
        SetLength(RanksJ, ValidCount);
        
        // Collect valid pairs
        ValidCount := 0;
        for K := 0 to FRowCount - 1 do
          if not (VarIsNull(ColI.Data[K]) or VarIsNull(ColJ.Data[K])) then
          begin
            ValuesI[ValidCount] := ColI.Data[K];
            ValuesJ[ValidCount] := ColJ.Data[K];
            IndicesI[ValidCount] := ValidCount;
            IndicesJ[ValidCount] := ValidCount;
            Inc(ValidCount);
          end;
          
        // Sort and rank first column
        QuickSortWithIndices(ValuesI, IndicesI, 0, ValidCount - 1);
        K := 0;
        while K < ValidCount do
        begin
          RankCount := 1;
          TempRank := K + 1;
          
          // Handle ties by averaging ranks
          while (K + RankCount < ValidCount) and (ValuesI[K + RankCount] = ValuesI[K]) do
          begin
            TempRank := TempRank + (K + RankCount + 1);
            Inc(RankCount);
          end;
          
          TempRank := TempRank / RankCount;
          
          // Assign average rank to all tied values
          for L := 0 to RankCount - 1 do
            RanksI[IndicesI[K + L]] := TempRank;
            
          Inc(K, RankCount);
        end;
        
        // Sort and rank second column
        QuickSortWithIndices(ValuesJ, IndicesJ, 0, ValidCount - 1);
        K := 0;
        while K < ValidCount do
        begin
          RankCount := 1;
          TempRank := K + 1;
          
          // Handle ties by averaging ranks
          while (K + RankCount < ValidCount) and (ValuesJ[K + RankCount] = ValuesJ[K]) do
          begin
            TempRank := TempRank + (K + RankCount + 1);
            Inc(RankCount);
          end;
          
          TempRank := TempRank / RankCount;
          
          // Assign average rank to all tied values
          for L := 0 to RankCount - 1 do
            RanksJ[IndicesJ[K + L]] := TempRank;
            
          Inc(K, RankCount);
        end;
        
        // Calculate Spearman correlation using ranks
        SumDiffSq := 0;
        for K := 0 to ValidCount - 1 do
          SumDiffSq := SumDiffSq + Sqr(RanksI[K] - RanksJ[K]);
          
        // Spearman correlation formula: ρ = 1 - (6 * Σd²) / (n * (n² - 1))
        Result.FColumns[I].Data[J] := 1 - (6 * SumDiffSq) / (ValidCount * (Sqr(ValidCount) - 1));
      end;
    end;
  except
    Result.Free;
    raise;
  end;
end;

function TDuckFrame.TryConvertValue(const Value: Variant; FromType, ToType: TDuckDBColumnType): Variant;
var
  TempDateTime: TDateTime;
begin
  // Handle null values
  if VarIsNull(Value) then
    Exit(Null);
    
  try
    case ToType of
      // Boolean conversion
      dctBoolean:
        Result := Boolean(Value);
        
      // Integer types
      dctTinyInt, dctSmallInt, dctInteger:
        Result := Integer(Value);
        
      dctBigInt:
        Result := Int64(Value);
        
      // Floating point types
      dctFloat, dctDouble, dctDecimal:
        Result := Double(Value);
        
      // Date and Time types
      // Date type
      dctDate:
        begin
          if VarIsType(Value, varDate) then
            Result := Value  // Keep the original date value
          else if TryStrToDateTime(VarToStr(Value), TempDateTime) then
            Result := Int(TempDateTime)
          else
            Result := 0;
        end;

      // Time type
      dctTime:
        begin
          if VarIsType(Value, varDate) then
            Result := Frac(VarToDateTime(Value))
          else if TryStrToTime(VarToStr(Value), TempDateTime) then
            Result := Frac(TempDateTime)
          else
            Result := 0;
        end;

      // Timestamp type (full datetime)
      dctTimestamp:
        begin
          if VarIsType(Value, varDate) then
            Result := Value  // Keep the original datetime value
          else if TryStrToDateTime(VarToStr(Value), TempDateTime) then
            Result := TempDateTime
          else
            Result := 0;
        end;
          
      // String and other types
      dctString, dctInterval, dctUUID, dctJSON:
        Result := VarToStr(Value);
        
      dctBlob:
        Result := Value;  // Keep BLOB data as-is
        
      else
        Result := Value;  // Pass through for unknown types
    end;
  except
    Result := Null;  // Return Null if conversion fails
  end;
end;

function TDuckFrame.GetCommonColumns(const Other: TDuckFrame): TStringArray;
var
  CommonColumns: TStringArray;
  I: Integer;
begin
  SetLength(CommonColumns, 0);
  for I := 0 to High(FColumns) do
  begin
    if Other.FindColumnIndex(FColumns[I].Name) >= 0 then
    begin
      SetLength(CommonColumns, Length(CommonColumns) + 1);
      CommonColumns[High(CommonColumns)] := FColumns[I].Name;
    end;
  end;
  Result := CommonColumns;
end;

function TDuckFrame.GetAllColumns(const Other: TDuckFrame): TStringArray;
var
  AllColumns: TStringArray;
  UniqueColumns: specialize THashSet<string>;
  I: Integer;
  CurrentIndex: Integer;
begin
  UniqueColumns := specialize THashSet<string>.Create;
  try
    // First, collect all unique column names
    for I := 0 to High(FColumns) do
      UniqueColumns.Add(FColumns[I].Name);
        
    for I := 0 to High(Other.FColumns) do
      UniqueColumns.Add(Other.FColumns[I].Name);
    
    // Create result array
    SetLength(AllColumns, UniqueColumns.Count);
    CurrentIndex := 0;
    
    // Add columns from first DataFrame
    for I := 0 to High(FColumns) do
    begin
      AllColumns[CurrentIndex] := FColumns[I].Name;
      Inc(CurrentIndex);
    end;
    
    // Add unique columns from second DataFrame
    for I := 0 to High(Other.FColumns) do
      if FindColumnIndex(Other.FColumns[I].Name) < 0 then
      begin
        AllColumns[CurrentIndex] := Other.FColumns[I].Name;
        Inc(CurrentIndex);
      end;
      
    Result := AllColumns;
  finally
    UniqueColumns.Free;
  end;
end;

function TDuckFrame.Union(const Other: TDuckFrame; Mode: TUnionMode = umStrict): TDuckFrame;
begin
  // First combine all rows
  Result := UnionAll(Other, Mode);
  // Then remove duplicates
  Result := Result.Distinct;
end;

function TDuckFrame.UnionAll(const Other: TDuckFrame; Mode: TUnionMode = umStrict): TDuckFrame;
var
  Row, Col, DestCol: Integer;
  SelectedColumns: TStringArray;
  ColMap: specialize TDictionary<string, Integer>;
  SourceCol: Integer;
begin
  Result := TDuckFrame.Create;
  ColMap := specialize TDictionary<string, Integer>.Create;
  try
    // Determine which columns to use based on mode
    case Mode of
      umStrict:
        if not HasSameStructure(Other) then
          raise EDuckDBError.Create('Cannot union DataFrames with different structures')
        else
          SelectedColumns := GetColumnNames;
          
      umCommon:
        SelectedColumns := GetCommonColumns(Other);
        
      umAll:
        SelectedColumns := GetAllColumns(Other);
    end;
    
    // Setup result structure - simple addition of row counts
    Result.FRowCount := FRowCount + Other.FRowCount;
    SetLength(Result.FColumns, Length(SelectedColumns));
    
    // Create column mapping
    for Col := 0 to High(SelectedColumns) do
    begin
      Result.FColumns[Col].Name := SelectedColumns[Col];
      
      // Determine column type
      if FindColumnIndex(SelectedColumns[Col]) >= 0 then
        Result.FColumns[Col].DataType := GetColumnByName(SelectedColumns[Col]).DataType
      else
        Result.FColumns[Col].DataType := Other.GetColumnByName(SelectedColumns[Col]).DataType;
      
      SetLength(Result.FColumns[Col].Data, Result.FRowCount);
      ColMap.Add(SelectedColumns[Col], Col);
    end;
    
    // Copy all rows from first DataFrame
    for Row := 0 to FRowCount - 1 do
      for Col := 0 to High(SelectedColumns) do
      begin
        DestCol := ColMap[SelectedColumns[Col]];
        SourceCol := FindColumnIndex(SelectedColumns[Col]);
        
        if SourceCol >= 0 then
          Result.FColumns[DestCol].Data[Row] := TryConvertValue(
            FColumns[SourceCol].Data[Row],
            FColumns[SourceCol].DataType,
            Result.FColumns[DestCol].DataType)
        else
          Result.FColumns[DestCol].Data[Row] := Null;
      end;
    
    // Copy all rows from second DataFrame
    for Row := 0 to Other.FRowCount - 1 do
      for Col := 0 to High(SelectedColumns) do
      begin
        DestCol := ColMap[SelectedColumns[Col]];
        SourceCol := Other.FindColumnIndex(SelectedColumns[Col]);
        
        if SourceCol >= 0 then
          Result.FColumns[DestCol].Data[FRowCount + Row] := TryConvertValue(
            Other.FColumns[SourceCol].Data[Row],
            Other.FColumns[SourceCol].DataType,
            Result.FColumns[DestCol].DataType)
        else
          Result.FColumns[DestCol].Data[FRowCount + Row] := Null;
      end;
  finally
    ColMap.Free;
  end;
end;

function TDuckFrame.HasSameStructure(const Other: TDuckFrame): Boolean;
var
  I: Integer;
begin
  Result := Length(FColumns) = Length(Other.FColumns);
  if not Result then
    Exit;
    
  for I := 0 to High(FColumns) do
  begin
    Result := Result and 
      (FColumns[I].Name = Other.FColumns[I].Name) and
      (FColumns[I].DataType = Other.FColumns[I].DataType);
    if not Result then
      Exit;
  end;
end;

function TDuckFrame.GetColumnNames: TStringArray;
var
  I: Integer;
begin
  SetLength(Result, Length(FColumns));
  for I := 0 to High(FColumns) do
    Result[I] := FColumns[I].Name;
end;

function TDuckFrame.Distinct: TDuckFrame;
var
  Row, Col, ResultRow: Integer;
  UniqueRows: specialize THashSet<string>;
  RowKey: string;
begin
  Result := TDuckFrame.Create;
  UniqueRows := specialize THashSet<string>.Create;
  try
    // First pass: count unique rows
    Result.FRowCount := 0;
    
    for Row := 0 to FRowCount - 1 do
    begin
      RowKey := '';
      for Col := 0 to High(FColumns) do
        RowKey := RowKey + VarToStr(FColumns[Col].Data[Row]);
      
      if UniqueRows.Add(RowKey) then  // Add returns true if the item was added (was unique)
        Inc(Result.FRowCount);
    end;
    
    // Setup result structure
    SetLength(Result.FColumns, Length(FColumns));
    UniqueRows.Clear; // Reset for second pass
    
    // Initialize columns
    for Col := 0 to High(FColumns) do
    begin
      Result.FColumns[Col].Name := FColumns[Col].Name;
      Result.FColumns[Col].DataType := FColumns[Col].DataType;
      SetLength(Result.FColumns[Col].Data, Result.FRowCount);
    end;
    
    // Second pass: copy unique rows
    ResultRow := 0;
    for Row := 0 to FRowCount - 1 do
    begin
      RowKey := '';
      for Col := 0 to High(FColumns) do
        RowKey := RowKey + VarToStr(FColumns[Col].Data[Row]);
      
      if UniqueRows.Add(RowKey) then  // Add returns true if the item was added (was unique)
      begin
        // Copy row data
        for Col := 0 to High(FColumns) do
          Result.FColumns[Col].Data[ResultRow] := FColumns[Col].Data[Row];
        Inc(ResultRow);
      end;
    end;
  finally
    UniqueRows.Free;
  end;
end;

constructor TDuckFrame.CreateBlank(const AColumnNames: array of string;
                                   const AColumnTypes: array of TDuckDBColumnType);
begin
  Create;  // Call default constructor
  InitializeBlank(AColumnNames, AColumnTypes);
end;

constructor TDuckFrame.CreateFromDuckDB(const ADatabase, ATableName: string);
var
  DB: p_duckdb_database;
  Conn: p_duckdb_connection;
  Result: duckdb_result;
  Query: string;
begin
  inherited Create;
  
  if duckdb_open(PAnsiChar(AnsiString(ADatabase)), @DB) <> DuckDBSuccess then
    raise EDuckDBError.Create('Failed to open database');
    
  try
    if duckdb_connect(DB, @Conn) <> DuckDBSuccess then
      raise EDuckDBError.Create('Failed to create connection');
      
    try
      Query := Format('SELECT * FROM %s', [ATableName]);
      if duckdb_query(Conn, PAnsiChar(AnsiString(Query)), @Result) <> DuckDBSuccess then
        raise EDuckDBError.CreateFmt('Failed to query table %s', [ATableName]);
        
      try
        LoadFromResult(@Result);
      finally
        duckdb_destroy_result(@Result);
      end;
    finally
      duckdb_disconnect(@Conn);
    end;
  finally
    duckdb_close(@DB);
  end;
end;

constructor TDuckFrame.CreateFromCSV(const AFileName: string;
                                   const AHasHeaders: Boolean = True;
                                   const ADelimiter: Char = ',');
var
  DB: p_duckdb_database;
  Conn: p_duckdb_connection;
  Result: duckdb_result;
  State: duckdb_state;
  SQLQuery: string;
  Options: string;
begin
  inherited Create;
  
  if not FileExists(AFileName) then
    raise EDuckDBError.CreateFmt('File not found: %s', [AFileName]);

  // Build options string
  Options := '';
  if not AHasHeaders then
    Options := Options + ', header=false';
  if ADelimiter <> ',' then
    Options := Options + Format(', delim=''%s''', [ADelimiter]);

  try
    // Open an in-memory database
    State := duckdb_open(nil, @DB);
    if State = DuckDBError then
      raise EDuckDBError.Create('Failed to create in-memory database');

    try
      // Create a connection
      State := duckdb_connect(DB, @Conn);
      if State = DuckDBError then
        raise EDuckDBError.Create('Failed to create connection');

      try
        // Create query with proper escaping
        SQLQuery := Format('SELECT * FROM read_csv_auto(''%s''%s)',
          [StringReplace(AFileName, '''', '''''', [rfReplaceAll]), Options]);

        // Execute query
        State := duckdb_query(Conn, PAnsiChar(AnsiString(SQLQuery)), @Result);
        if State = DuckDBError then
          raise EDuckDBError.Create('Failed to read CSV file');

        try
          // Load the result into our frame
          LoadFromResult(@Result);
        finally
          duckdb_destroy_result(@Result);
        end;

      finally
        duckdb_disconnect(@Conn);
      end;

    finally
      duckdb_close(@DB);
    end;

  except
    Clear;  // Clean up if something went wrong
    raise;
  end;
end;

procedure TDuckFrame.InitializeBlank(const AColumnNames: array of string;
                                   const AColumnTypes: array of TDuckDBColumnType);
var
  I : Integer;
begin
  if Length(AColumnNames) <> Length(AColumnTypes) then
    raise EDuckDBError.Create('Column names and types arrays must have same length');
    
  Clear;
  SetLength(FColumns, Length(AColumnNames));
  
  for I := 0 to High(AColumnNames) do
  begin
    FColumns[I].Name := AColumnNames[I];
    FColumns[I].DataType := AColumnTypes[I];
    SetLength(FColumns[I].Data, 0);
  end;
  
  FRowCount := 0;
end;

procedure TDuckFrame.AddColumn(const AName: string; AType: TDuckDBColumnType);
var
  I, NewCol: Integer;
begin
  // Check if column name already exists
  if FindColumnIndex(AName) >= 0 then
    raise EDuckDBError.CreateFmt('Column %s already exists', [AName]);
    
  NewCol := Length(FColumns);
  SetLength(FColumns, NewCol + 1);
  
  FColumns[NewCol].Name := AName;
  FColumns[NewCol].DataType := AType;
  SetLength(FColumns[NewCol].Data, FRowCount);
  
  // Initialize new column with null values
  for I := 0 to FRowCount - 1 do
    FColumns[NewCol].Data[I] := Null;
end;

procedure TDuckFrame.AddRow(const AValues: array of Variant);
var
  I, NewRow: Integer;
begin
  if Length(AValues) <> Length(FColumns) then
    raise EDuckDBError.Create('Number of values must match number of columns');
    
  NewRow := FRowCount;
  Inc(FRowCount);
  
  // Resize all columns
  for I := 0 to High(FColumns) do
  begin
    SetLength(FColumns[I].Data, FRowCount);
    FColumns[I].Data[NewRow] := TryConvertValue(AValues[I], 
                                               dctUnknown, 
                                               FColumns[I].DataType);
  end;
end;

procedure TDuckFrame.SetValue(const ARow: Integer; const AColumnName: string; const AValue: Variant);
var
  ColIndex: Integer;
begin
  if (ARow < 0) or (ARow >= FRowCount) then
    raise EDuckDBError.Create('Row index out of range');
    
  ColIndex := FindColumnIndex(AColumnName);
  if ColIndex < 0 then
    raise EDuckDBError.CreateFmt('Column %s not found', [AColumnName]);
    
  FColumns[ColIndex].Data[ARow] := TryConvertValue(AValue,
                                                   dctUnknown,
                                                  FColumns[ColIndex].DataType);
end;

{ Converts a DuckDB column type to its SQL string representation }
function DuckDBTypeToString(ColumnType: TDuckDBColumnType): string;
begin
  case ColumnType of
    dctUnknown: Result := 'UNKNOWN';
    dctBoolean: Result := 'BOOLEAN';
    dctTinyInt: Result := 'TINYINT';
    dctSmallInt: Result := 'SMALLINT';
    dctInteger: Result := 'INTEGER';
    dctBigInt: Result := 'BIGINT';
    dctFloat: Result := 'FLOAT';
    dctDouble: Result := 'DOUBLE';
    dctDate: Result := 'DATE';
    dctTime: Result := 'TIME';
    dctTimestamp: Result := 'TIMESTAMP';
    dctInterval: Result := 'INTERVAL';
    dctString: Result := 'VARCHAR';
    dctBlob: Result := 'BLOB';
    dctDecimal: Result := 'DECIMAL';
    dctUUID: Result := 'UUID';
    dctJSON: Result := 'JSON';
  else
    Result := 'UNKNOWN';
  end;
end;

{ Converts a SQL type string to its DuckDB column type }
function StringToDuckDBType(const TypeName: string): TDuckDBColumnType;
begin
  if SameText(TypeName, 'BOOLEAN') then Result := dctBoolean
  else if SameText(TypeName, 'TINYINT') then Result := dctTinyInt
  else if SameText(TypeName, 'SMALLINT') then Result := dctSmallInt
  else if SameText(TypeName, 'INTEGER') or SameText(TypeName, 'INT') then Result := dctInteger
  else if SameText(TypeName, 'BIGINT') then Result := dctBigInt
  else if SameText(TypeName, 'FLOAT') then Result := dctFloat
  else if SameText(TypeName, 'DOUBLE') then Result := dctDouble
  else if SameText(TypeName, 'DATE') then Result := dctDate
  else if SameText(TypeName, 'TIME') then Result := dctTime
  else if SameText(TypeName, 'TIMESTAMP') then Result := dctTimestamp
  else if SameText(TypeName, 'INTERVAL') then Result := dctInterval
  else if SameText(TypeName, 'VARCHAR') or SameText(TypeName, 'STRING') then Result := dctString
  else if SameText(TypeName, 'BLOB') then Result := dctBlob
  else if SameText(TypeName, 'DECIMAL') or SameText(TypeName, 'NUMERIC') then Result := dctDecimal
  else if SameText(TypeName, 'UUID') then Result := dctUUID
  else if SameText(TypeName, 'JSON') then Result := dctJSON
  else Result := dctUnknown;
end;

constructor TDuckFrame.CreateFromParquet(const AFileName: string);
var
  DB: p_duckdb_database;
  Conn: p_duckdb_connection;
  Result: duckdb_result;
  State: duckdb_state;
  SQLQuery: string;
begin
  inherited Create;
  
  if not FileExists(AFileName) then
    raise EDuckDBError.Create('File not found: ' + AFileName);

  try
    // Open an in-memory database
    State := duckdb_open(nil, @DB);
    if State = DuckDBError then
      raise EDuckDBError.Create('Failed to create in-memory database');

    try
      // Create a connection
      State := duckdb_connect(DB, @Conn);
      if State = DuckDBError then
        raise EDuckDBError.Create('Failed to create connection');

      try
        // Create query with proper escaping
        SQLQuery := Format('SELECT * FROM read_parquet(''%s'')',
          [StringReplace(AFileName, '''', '''''', [rfReplaceAll])]);

        // Execute query
        State := duckdb_query(Conn, PAnsiChar(AnsiString(SQLQuery)), @Result);
        if State = DuckDBError then
          raise EDuckDBError.Create('Failed to read Parquet file');

        try
          LoadFromResult(@Result);
        finally
          duckdb_destroy_result(@Result);
        end;

      finally
        duckdb_disconnect(@Conn);
      end;

    finally
      duckdb_close(@DB);
    end;

  except
    on E: Exception do
    begin
      Free;
      raise;
    end;
  end;
end;

constructor TDuckFrame.CreateFromParquet(const Files: array of string);
var
  DB: p_duckdb_database;
  Conn: p_duckdb_connection;
  Result: duckdb_result;
  State: duckdb_state;
  SQLQuery: string;
  FileList: string;
  I: Integer;
begin
  inherited Create;
  
  if Length(Files) = 0 then
    raise EDuckDBError.Create('No files specified for Parquet reading');

  try
    // Open an in-memory database
    State := duckdb_open(nil, @DB);
    if State = DuckDBError then
      raise EDuckDBError.Create('Failed to create in-memory database');

    try
      // Create a connection
      State := duckdb_connect(DB, @Conn);
      if State = DuckDBError then
        raise EDuckDBError.Create('Failed to create connection');

      try
        // Build file list string
        FileList := '[';
        for I := 0 to High(Files) do
        begin
          if I > 0 then
            FileList := FileList + ', ';
          FileList := FileList + Format('''%s''',
            [StringReplace(Files[I], '''', '''''', [rfReplaceAll])]);
        end;
        FileList := FileList + ']';

        // Create query
        SQLQuery := Format('SELECT * FROM read_parquet(%s)', [FileList]);

        // Execute query
        State := duckdb_query(Conn, PAnsiChar(AnsiString(SQLQuery)), @Result);
        if State = DuckDBError then
          raise EDuckDBError.Create('Failed to read Parquet files');

        try
          // Load the result into our frame
          LoadFromResult(@Result);
        finally
          duckdb_destroy_result(@Result);
        end;

      finally
        duckdb_disconnect(@Conn);
      end;

    finally
      duckdb_close(@DB);
    end;

  except
    on E: Exception do
    begin
      Free;  // Clean up if constructor fails
      raise;
    end;
  end;
end;

function TDuckFrame.Filter(const ColumnName: string; const Value: Variant): TDuckFrame;
begin
  Result := Filter(ColumnName, '=', Value);
end;

function TDuckFrame.Filter(const ColumnName: string; const CompareOp: string; 
  const Value: Variant): TDuckFrame;
var
  I, ColIndex, NewRowCount: Integer;
  MatchFound: Boolean;
  Row: Integer;
begin
  Result := TDuckFrame.Create;
  try
    ColIndex := FindColumnIndex(ColumnName);
    if ColIndex = -1 then
      raise EDuckDBError.CreateFmt('Column "%s" not found', [ColumnName]);

    // First pass: count matching rows
    NewRowCount := 0;
    for I := 0 to FRowCount - 1 do
    begin
      MatchFound := False;
      
      if VarIsNull(FColumns[ColIndex].Data[I]) then
        Continue;
        
      case AnsiLowerCase(CompareOp) of
        '=', '==': MatchFound := FColumns[ColIndex].Data[I] = Value;
        '<>','!=': MatchFound := FColumns[ColIndex].Data[I] <> Value;
        '<': MatchFound := FColumns[ColIndex].Data[I] < Value;
        '>': MatchFound := FColumns[ColIndex].Data[I] > Value;
        '<=': MatchFound := FColumns[ColIndex].Data[I] <= Value;
        '>=': MatchFound := FColumns[ColIndex].Data[I] >= Value;
        else
          raise EDuckDBError.CreateFmt('Unsupported comparison operator: %s', [CompareOp]);
      end;
      
      if MatchFound then
        Inc(NewRowCount);
    end;

    // Initialize result frame
    Result.FRowCount := NewRowCount;
    SetLength(Result.FColumns, Length(FColumns));
    
    for I := 0 to Length(FColumns) - 1 do
    begin
      Result.FColumns[I].Name := FColumns[I].Name;
      Result.FColumns[I].DataType := FColumns[I].DataType;
      SetLength(Result.FColumns[I].Data, NewRowCount);
    end;

    // Second pass: copy matching rows
    Row := 0;
    for I := 0 to FRowCount - 1 do
    begin
      MatchFound := False;
      
      if not VarIsNull(FColumns[ColIndex].Data[I]) then
      begin
        case AnsiLowerCase(CompareOp) of
          '=', '==': MatchFound := FColumns[ColIndex].Data[I] = Value;
          '<>','!=': MatchFound := FColumns[ColIndex].Data[I] <> Value;
          '<': MatchFound := FColumns[ColIndex].Data[I] < Value;
          '>': MatchFound := FColumns[ColIndex].Data[I] > Value;
          '<=': MatchFound := FColumns[ColIndex].Data[I] <= Value;
          '>=': MatchFound := FColumns[ColIndex].Data[I] >= Value;
        end;
      end;
      
      if MatchFound then
      begin
        for ColIndex := 0 to Length(FColumns) - 1 do
          Result.FColumns[ColIndex].Data[Row] := FColumns[ColIndex].Data[I];
        Inc(Row);
      end;
    end;
  except
    Result.Free;
    raise;
  end;
end;

function TDuckFrame.Sort(const ColumnName: string; Ascending: Boolean = True): TDuckFrame;
var
  SortCols: array of string;
  SortAsc: array of Boolean;
begin
  SetLength(SortCols, 1);
  SetLength(SortAsc, 1);
  SortCols[0] := ColumnName;
  SortAsc[0] := Ascending;
  Result := Sort(SortCols, SortAsc);
end;

function TDuckFrame.Sort(const ColumnNames: array of string; 
  const Ascending: array of Boolean): TDuckFrame;
var
  I, J, ColIndex: Integer;
  Indices: array of Integer;
  RealValues: array of Double;
  StrValues: array of string;
  ColType: TDuckDBColumnType;
begin
  Result := TDuckFrame.Create;
  try
    if Length(ColumnNames) = 0 then
      raise EDuckDBError.Create('No columns specified for sorting');
    if Length(ColumnNames) <> Length(Ascending) then
      raise EDuckDBError.Create('Number of columns and sort directions must match');

    // Initialize indices
    SetLength(Indices, FRowCount);
    for I := 0 to FRowCount - 1 do
      Indices[I] := I;

    // Sort for each column in reverse order (to maintain stable sort)
    for I := High(ColumnNames) downto 0 do
    begin
      ColIndex := FindColumnIndex(ColumnNames[I]);
      if ColIndex = -1 then
        raise EDuckDBError.CreateFmt('Column "%s" not found', [ColumnNames[I]]);

      ColType := FColumns[ColIndex].DataType;
      
      // Prepare RealValues for sorting
      case ColType of
        dctString:
          begin
            SetLength(StrValues, FRowCount);
            for J := 0 to FRowCount - 1 do
              if VarIsNull(FColumns[ColIndex].Data[Indices[J]]) then
                StrValues[J] := ''
              else
                StrValues[J] := VarToStr(FColumns[ColIndex].Data[Indices[J]]);
            // Sort string RealValues
            QuickSortWithStrings(StrValues, Indices, 0, FRowCount - 1, Ascending[I]);
          end;
        else
          begin
           // Convert to numeric for all other types
            SetLength(RealValues, FRowCount);
            for J := 0 to FRowCount - 1 do
              if VarIsNull(FColumns[ColIndex].Data[Indices[J]]) then
                RealValues[J] := Infinity  // Use Infinity for ascending sort to put nulls at end
              else
                RealValues[J] := VarAsType(FColumns[ColIndex].Data[Indices[J]], varDouble);
            // Sort numeric RealValues
            QuickSortWithIndices(RealValues, Indices, 0, FRowCount - 1, Ascending[I]);
          end;
      end;
    end;

    // Create sorted result
    Result.FRowCount := FRowCount;
    SetLength(Result.FColumns, Length(FColumns));
    
    for I := 0 to High(FColumns) do
    begin
      Result.FColumns[I].Name := FColumns[I].Name;
      Result.FColumns[I].DataType := FColumns[I].DataType;
      SetLength(Result.FColumns[I].Data, FRowCount);
      
      for J := 0 to FRowCount - 1 do
        Result.FColumns[I].Data[J] := FColumns[I].Data[Indices[J]];
    end;
  except
    Result.Free;
    raise;
  end;
end;


function TDuckFrame.Sample(Count: Integer): TDuckFrame;
var
  I, J, RandIndex: Integer;
  UsedIndices: array of Boolean;
begin
  Result := TDuckFrame.Create;
  try
    if Count > FRowCount then
      Count := FRowCount;
      
    if Count <= 0 then
      raise EDuckDBError.Create('Sample count must be positive');

    // Initialize result structure
    Result.FRowCount := Count;
    SetLength(Result.FColumns, Length(FColumns));
    
    for I := 0 to High(FColumns) do
    begin
      Result.FColumns[I].Name := FColumns[I].Name;
      Result.FColumns[I].DataType := FColumns[I].DataType;
      SetLength(Result.FColumns[I].Data, Count);
    end;

    // Track used indices to avoid duplicates
    SetLength(UsedIndices, FRowCount);
    for I := 0 to High(UsedIndices) do
      UsedIndices[I] := False;

    // Random sampling without replacement
    Randomize;  // Initialize random number generator
    for I := 0 to Count - 1 do
    begin
      repeat
        RandIndex := Random(FRowCount);
      until not UsedIndices[RandIndex];
      
      UsedIndices[RandIndex] := True;
      
      // Copy row data
      for J := 0 to High(FColumns) do
        Result.FColumns[J].Data[I] := FColumns[J].Data[RandIndex];
    end;
  except
    Result.Free;
    raise;
  end;
end;

function TDuckFrame.Sample(Percentage: Double): TDuckFrame;
var
  SampleCount: Integer;
begin
  if (Percentage <= 0) or (Percentage > 100) then
    raise EDuckDBError.Create('Percentage must be between 0 and 100');
    
  SampleCount := Round((Percentage / 100) * FRowCount);
  if SampleCount = 0 then
    SampleCount := 1;
    
  Result := Sample(SampleCount);
end;

function TDuckFrame.GroupBy(const ColumnNames: array of string): TDuckFrame;
var
  I, J, ColIndex: Integer;
  GroupMap: specialize TDictionary<string, Integer>;
  GroupKey: string;
  CurrentGroup: Integer;
begin
  Result := TDuckFrame.Create;
  GroupMap := specialize TDictionary<string, Integer>.Create;
  try
    // Validate columns exist
    for I := 0 to High(ColumnNames) do
      if FindColumnIndex(ColumnNames[I]) < 0 then
        raise EDuckDBError.CreateFmt('Column "%s" not found', [ColumnNames[I]]);

    // Build group keys and count unique groups
    for I := 0 to FRowCount - 1 do
    begin
      GroupKey := '';
      for J := 0 to High(ColumnNames) do
      begin
        ColIndex := FindColumnIndex(ColumnNames[J]);
        GroupKey := GroupKey + VarToStr(FColumns[ColIndex].Data[I]) + #0;
      end;
      
      if not GroupMap.ContainsKey(GroupKey) then
        GroupMap.Add(GroupKey, GroupMap.Count);
    end;

    // Setup result structure
    Result.FRowCount := GroupMap.Count;
    SetLength(Result.FColumns, Length(ColumnNames) + 1); // +1 for count column
    
    // Setup group columns
    for I := 0 to High(ColumnNames) do
    begin
      ColIndex := FindColumnIndex(ColumnNames[I]);
      Result.FColumns[I].Name := ColumnNames[I];
      Result.FColumns[I].DataType := FColumns[ColIndex].DataType;
      SetLength(Result.FColumns[I].Data, Result.FRowCount);
    end;
    
    // Setup count column
    Result.FColumns[High(Result.FColumns)].Name := 'Count';
    Result.FColumns[High(Result.FColumns)].DataType := dctInteger;
    SetLength(Result.FColumns[High(Result.FColumns)].Data, Result.FRowCount);

    // Reset map for second pass
    GroupMap.Clear;
    
    // Fill result data
    for I := 0 to FRowCount - 1 do
    begin
      GroupKey := '';
      for J := 0 to High(ColumnNames) do
      begin
        ColIndex := FindColumnIndex(ColumnNames[J]);
        GroupKey := GroupKey + VarToStr(FColumns[ColIndex].Data[I]) + #0;
      end;
      
      if not GroupMap.ContainsKey(GroupKey) then
      begin
        CurrentGroup := GroupMap.Count;
        GroupMap.Add(GroupKey, CurrentGroup);
        
        // Fill group values
        for J := 0 to High(ColumnNames) do
        begin
          ColIndex := FindColumnIndex(ColumnNames[J]);
          Result.FColumns[J].Data[CurrentGroup] := FColumns[ColIndex].Data[I];
        end;
        Result.FColumns[High(Result.FColumns)].Data[CurrentGroup] := 1;
      end
      else
      begin
        CurrentGroup := GroupMap[GroupKey];
        Result.FColumns[High(Result.FColumns)].Data[CurrentGroup] :=
          Result.FColumns[High(Result.FColumns)].Data[CurrentGroup] + 1;
      end;
    end;
  finally
    GroupMap.Free;
  end;
end;

function TDuckFrame.RenameColumn(const OldName, NewName: string): TDuckFrame;
var
  I, ColIndex: Integer;
begin
  Result := TDuckFrame.Create;
  try
    // Check if old column exists
    ColIndex := FindColumnIndex(OldName);
    if ColIndex < 0 then
      raise EDuckDBError.CreateFmt('Column "%s" not found', [OldName]);
      
    // Check if new name already exists
    if (OldName <> NewName) and (FindColumnIndex(NewName) >= 0) then
      raise EDuckDBError.CreateFmt('Column "%s" already exists', [NewName]);

    // Copy structure
    Result.FRowCount := FRowCount;
    SetLength(Result.FColumns, Length(FColumns));
    
    // Copy data and rename column
    for I := 0 to High(FColumns) do
    begin
      Result.FColumns[I].DataType := FColumns[I].DataType;
      if I = ColIndex then
        Result.FColumns[I].Name := NewName
      else
        Result.FColumns[I].Name := FColumns[I].Name;
        
      SetLength(Result.FColumns[I].Data, FRowCount);
      Move(FColumns[I].Data[0], Result.FColumns[I].Data[0], 
           FRowCount * SizeOf(Variant));
    end;
  except
    Result.Free;
    raise;
  end;
end;

function TDuckFrame.DropColumns(const ColumnNames: array of string): TDuckFrame;
var
  I, J, NewColCount: Integer;
  ColsToKeep: array of Boolean;
begin
  Result := TDuckFrame.Create;
  try
    // Mark columns to keep
    SetLength(ColsToKeep, Length(FColumns));
    NewColCount := Length(FColumns);
    
    // Initialize all columns as "keep"
    for I := 0 to High(ColsToKeep) do
      ColsToKeep[I] := True;
    
    // Mark columns to drop
    for I := 0 to High(ColumnNames) do
    begin
      J := FindColumnIndex(ColumnNames[I]);
      if J >= 0 then
      begin
        if ColsToKeep[J] then
        begin
          ColsToKeep[J] := False;
          Dec(NewColCount);
        end;
      end;
    end;

    // Check if we're dropping all columns
    if NewColCount = 0 then
      raise EDuckDBError.Create('Cannot drop all columns');

    // Initialize result frame
    Result.FRowCount := FRowCount;
    SetLength(Result.FColumns, NewColCount);
    
    // Copy remaining columns
    NewColCount := 0;
    for I := 0 to High(FColumns) do
    begin
      if ColsToKeep[I] then
      begin
        Result.FColumns[NewColCount].Name := FColumns[I].Name;
        Result.FColumns[NewColCount].DataType := FColumns[I].DataType;
        SetLength(Result.FColumns[NewColCount].Data, FRowCount);
        
        // Use Move for better performance when copying data
        Move(FColumns[I].Data[0], Result.FColumns[NewColCount].Data[0],
             FRowCount * SizeOf(Variant));
             
        Inc(NewColCount);
      end;
    end;
  except
    Result.Free;
    raise;
  end;
end;

function TDuckFrame.ValueCounts(const ColumnName: string; 
  Normalize: Boolean = False): TDuckFrame;
var
  FreqMap: specialize TDictionary<Variant, Integer>;
  Total: Integer;
  I: Integer;
  Value: Variant;
  ColIndex: Integer;
begin
  Result := nil;  // Initialize to nil for safety
  FreqMap := specialize TDictionary<Variant, Integer>.Create;
  try
    // Find column
    ColIndex := FindColumnIndex(ColumnName);
    if ColIndex < 0 then
      raise EDuckDBError.CreateFmt('Column "%s" not found', [ColumnName]);

    // Count occurrences
    Total := 0;
    for I := 0 to FRowCount - 1 do
    begin
      Value := FColumns[ColIndex].Data[I];
      if not VarIsNull(Value) then
      begin
        if FreqMap.ContainsKey(Value) then
          FreqMap[Value] := FreqMap[Value] + 1
        else
          FreqMap.Add(Value, 1);
        Inc(Total);
      end;
    end;

    // Create result DataFrame
    Result := TDuckFrame.Create;
    SetLength(Result.FColumns, 3);
    
    // Initialize columns properly
    with Result.FColumns[0] do
    begin
      Name := 'Value';
      DataType := FColumns[ColIndex].DataType;
      SetLength(Data, FreqMap.Count);
    end;
    
    with Result.FColumns[1] do
    begin
      Name := 'Count';
      DataType := dctInteger;
      SetLength(Data, FreqMap.Count);
    end;
    
    with Result.FColumns[2] do
    begin
      Name := 'Percentage';
      DataType := dctDouble;
      SetLength(Data, FreqMap.Count);
    end;

    Result.FRowCount := FreqMap.Count;

    // Fill results
    I := 0;
    for Value in FreqMap.Keys do
    begin
      Result.FColumns[0].Data[I] := Value;
      Result.FColumns[1].Data[I] := FreqMap[Value];
      if Normalize then
        Result.FColumns[2].Data[I] := (FreqMap[Value] / Total) * 100
      else
        Result.FColumns[2].Data[I] := FreqMap[Value];
      Inc(I);
    end;

    // Sort by count descending
    Result := Result.Sort('Count', False);
  except
    FreeAndNil(Result);  // Clean up on error
    raise;
  end;
  FreqMap.Free;
end;


function TDuckFrame.Quantile(const ColumnName: string; const Quantiles: array of Double): TDuckFrame;
var
  Col: TDuckDBColumn;
  NumericValues: array of Double;
  ValidCount, I: Integer;
  Value: Variant;
begin
  Result := TDuckFrame.Create;
  try
    // Find the column
    Col := GetColumnByName(ColumnName);
    if not IsNumericColumn(Col) then
      raise EDuckDBError.CreateFmt('Column "%s" is not numeric', [ColumnName]);

    // Initialize arrays
    SetLength(NumericValues, FRowCount);
    ValidCount := 0;

    // Collect valid numeric values
    for I := 0 to FRowCount - 1 do
    begin
      Value := Col.Data[I];
      if not VarIsNull(Value) then
      begin
        NumericValues[ValidCount] := VarAsType(Value, varDouble);
        Inc(ValidCount);
      end;
    end;
    SetLength(NumericValues, ValidCount);

    // Sort values for percentile calculation
    QuickSort(NumericValues, 0, ValidCount - 1);

    // Create result DataFrame
    SetLength(Result.FColumns, 2);
    Result.FColumns[0].Name := 'quantile';
    Result.FColumns[0].DataType := dctDouble;
    Result.FColumns[1].Name := ColumnName;
    Result.FColumns[1].DataType := dctDouble;

    // Calculate quantiles
    Result.FRowCount := Length(Quantiles);
    SetLength(Result.FColumns[0].Data, Result.FRowCount);
    SetLength(Result.FColumns[1].Data, Result.FRowCount);

    for I := 0 to High(Quantiles) do
    begin
      if (Quantiles[I] < 0) or (Quantiles[I] > 1) then
        raise EDuckDBError.Create('Quantiles must be between 0 and 1');
        
      Result.FColumns[0].Data[I] := Quantiles[I];
      Result.FColumns[1].Data[I] := CalculatePercentile(NumericValues, Quantiles[I]);
    end;
  except
    Result.Free;
    raise;
  end;
end;

function TDuckFrame.Join(Other: TDuckFrame; Mode: TJoinMode = jmLeftJoin): TDuckFrame;
var
  CommonCols: array of string;
  ColMap: array of TJoinColumnMap;
  I, J: Integer;
begin
  Result := nil;
  try
    // Find common columns
    SetLength(CommonCols, 0);
    SetLength(ColMap, 0);
    for I := 0 to High(FColumns) do
      for J := 0 to High(Other.FColumns) do
        if FColumns[I].Name = Other.FColumns[J].Name then
        begin
          SetLength(CommonCols, Length(CommonCols) + 1);
          SetLength(ColMap, Length(ColMap) + 1);
          CommonCols[High(CommonCols)] := FColumns[I].Name;
          ColMap[High(ColMap)].LeftIndex := I;
          ColMap[High(ColMap)].RightIndex := J;
        end;

    if Length(CommonCols) = 0 then
      raise EDuckDBError.Create('No common columns found for join');

    // Create result frame with all needed columns
    Result := TDuckFrame.Create;
    SetLength(Result.FColumns, Length(FColumns) + Length(Other.FColumns) - Length(CommonCols));

    // Copy column definitions
    for I := 0 to High(FColumns) do
    begin
      Result.FColumns[I].Name := FColumns[I].Name;
      Result.FColumns[I].DataType := FColumns[I].DataType;
    end;

    // Copy non-common columns from right frame
    J := Length(FColumns);
    for I := 0 to High(Other.FColumns) do
      if not Contains(CommonCols, Other.FColumns[I].Name) then
      begin
        Result.FColumns[J].Name := Other.FColumns[I].Name;
        Result.FColumns[J].DataType := Other.FColumns[I].DataType;
        Inc(J);
      end;

    case Mode of
      jmInner: PerformInnerJoin(Result, Other, CommonCols, ColMap);
      jmLeftJoin: PerformLeftJoin(Result, Other, CommonCols, ColMap);
      jmRightJoin: PerformRightJoin(Result, Other, CommonCols, ColMap);
      jmFullJoin: PerformFullJoin(Result, Other, CommonCols, ColMap);
    end;
  except
    FreeAndNil(Result);
    raise;
  end;
end;


procedure TDuckFrame.PerformLeftJoin(Result: TDuckFrame; Other: TDuckFrame;
  const CommonCols: array of string; const ColMap: array of TJoinColumnMap);
var
  I, J, K, ResultRow: Integer;
  HasMatch: Boolean;
begin
  // Pre-allocate for worst case
  for I := 0 to High(Result.FColumns) do
    SetLength(Result.FColumns[I].Data, FRowCount);

  Result.FRowCount := 0;
  
  // Process each left row
  for I := 0 to FRowCount - 1 do
  begin
    HasMatch := False;
    
    // Look for matches in right frame
    for J := 0 to Other.FRowCount - 1 do
    begin
      // Check if rows match on common columns
      HasMatch := True;
      for K := 0 to High(ColMap) do
        if FColumns[ColMap[K].LeftIndex].Data[I] <> 
           Other.FColumns[ColMap[K].RightIndex].Data[J] then
        begin
          HasMatch := False;
          Break;
        end;
        
      if HasMatch then
      begin
        ResultRow := Result.FRowCount;
        // Copy all columns from left frame
        for K := 0 to High(FColumns) do
          Result.FColumns[K].Data[ResultRow] := FColumns[K].Data[I];
          
        // Copy non-common columns from right frame
        for K := 0 to High(Other.FColumns) do
          if not Contains(CommonCols, Other.FColumns[K].Name) then
            Result.FColumns[Length(FColumns) + K].Data[ResultRow] := 
              Other.FColumns[K].Data[J];
              
        Inc(Result.FRowCount);
      end;
    end;
    
    // If no match, add left row with nulls for right columns
    if not HasMatch then
    begin
      ResultRow := Result.FRowCount;
      // Copy left frame columns
      for K := 0 to High(FColumns) do
        Result.FColumns[K].Data[ResultRow] := FColumns[K].Data[I];
        
      // Set nulls for right frame columns
      for K := Length(FColumns) to High(Result.FColumns) do
        Result.FColumns[K].Data[ResultRow] := Null;
        
      Inc(Result.FRowCount);
    end;
  end;
end;
procedure TDuckFrame.PerformInnerJoin(Result: TDuckFrame; Other: TDuckFrame;
  const CommonCols: array of string; const ColMap: array of TJoinColumnMap);
var
  I, J, K, ResultRow: Integer;
  IsMatch: Boolean;
begin
  // Pre-allocate for worst case
  for I := 0 to High(Result.FColumns) do
    SetLength(Result.FColumns[I].Data, FRowCount * Other.FRowCount);

  Result.FRowCount := 0;
  
  // Process each left row
  for I := 0 to FRowCount - 1 do
    // Compare with each right row
    for J := 0 to Other.FRowCount - 1 do
    begin
      // Check if rows match on common columns
      IsMatch := True;
      for K := 0 to High(ColMap) do
        if FColumns[ColMap[K].LeftIndex].Data[I] <> 
           Other.FColumns[ColMap[K].RightIndex].Data[J] then
        begin
          IsMatch := False;
          Break;
        end;
        
      if IsMatch then
      begin
        ResultRow := Result.FRowCount;
        // Copy all columns from left frame
        for K := 0 to High(FColumns) do
          Result.FColumns[K].Data[ResultRow] := FColumns[K].Data[I];
          
        // Copy non-common columns from right frame
        for K := 0 to High(Other.FColumns) do
          if not Contains(CommonCols, Other.FColumns[K].Name) then
            Result.FColumns[Length(FColumns) + K].Data[ResultRow] := 
              Other.FColumns[K].Data[J];
              
        Inc(Result.FRowCount);
      end;
    end;
end;

procedure TDuckFrame.PerformRightJoin(Result: TDuckFrame; Other: TDuckFrame;
  const CommonCols: array of string; const ColMap: array of TJoinColumnMap);
var
  I, J, K, ResultRow: Integer;
  HasMatch: Boolean;
begin
  // Pre-allocate for worst case
  for I := 0 to High(Result.FColumns) do
    SetLength(Result.FColumns[I].Data, Other.FRowCount);

  Result.FRowCount := 0;
  
  // Process each right row
  for J := 0 to Other.FRowCount - 1 do
  begin
    HasMatch := False;
    
    // Look for matches in left frame
    for I := 0 to FRowCount - 1 do
    begin
      // Check if rows match on common columns
      HasMatch := True;
      for K := 0 to High(ColMap) do
        if FColumns[ColMap[K].LeftIndex].Data[I] <> 
           Other.FColumns[ColMap[K].RightIndex].Data[J] then
        begin
          HasMatch := False;
          Break;
        end;
        
      if HasMatch then
      begin
        ResultRow := Result.FRowCount;
        // Copy all columns from left frame
        for K := 0 to High(FColumns) do
          Result.FColumns[K].Data[ResultRow] := FColumns[K].Data[I];
          
        // Copy non-common columns from right frame
        for K := 0 to High(Other.FColumns) do
          if not Contains(CommonCols, Other.FColumns[K].Name) then
            Result.FColumns[Length(FColumns) + K].Data[ResultRow] := 
              Other.FColumns[K].Data[J];
              
        Inc(Result.FRowCount);
      end;
    end;
    
    // If no match, add right row with nulls for left columns
    if not HasMatch then
    begin
      ResultRow := Result.FRowCount;
      // Set nulls for left frame columns
      for K := 0 to High(FColumns) do
        Result.FColumns[K].Data[ResultRow] := Null;
        
      // Copy right frame columns
      for K := 0 to High(Other.FColumns) do
        if not Contains(CommonCols, Other.FColumns[K].Name) then
          Result.FColumns[Length(FColumns) + K].Data[ResultRow] := 
            Other.FColumns[K].Data[J];
            
      Inc(Result.FRowCount);
    end;
  end;
end;

procedure TDuckFrame.PerformFullJoin(Result: TDuckFrame; Other: TDuckFrame;
  const CommonCols: array of string; const ColMap: array of TJoinColumnMap);
var
  I, J, K, ResultRow: Integer;
  MatchedRight: array of Boolean;
  HasMatch: Boolean;
begin
  // Track which right rows have been matched
  SetLength(MatchedRight, Other.FRowCount);
  for I := 0 to High(MatchedRight) do
    MatchedRight[I] := False;

  // Pre-allocate for worst case
  for I := 0 to High(Result.FColumns) do
    SetLength(Result.FColumns[I].Data, FRowCount + Other.FRowCount);

  Result.FRowCount := 0;
  
  // First do left join
  for I := 0 to FRowCount - 1 do
  begin
    HasMatch := False;
    
    for J := 0 to Other.FRowCount - 1 do
    begin
      // Check if rows match on common columns
      HasMatch := True;
      for K := 0 to High(ColMap) do
        if FColumns[ColMap[K].LeftIndex].Data[I] <> 
           Other.FColumns[ColMap[K].RightIndex].Data[J] then
        begin
          HasMatch := False;
          Break;
        end;
        
      if HasMatch then
      begin
        MatchedRight[J] := True;
        ResultRow := Result.FRowCount;
        // Copy all columns from left frame
        for K := 0 to High(FColumns) do
          Result.FColumns[K].Data[ResultRow] := FColumns[K].Data[I];
          
        // Copy non-common columns from right frame
        for K := 0 to High(Other.FColumns) do
          if not Contains(CommonCols, Other.FColumns[K].Name) then
            Result.FColumns[Length(FColumns) + K].Data[ResultRow] := 
              Other.FColumns[K].Data[J];
              
        Inc(Result.FRowCount);
      end;
    end;
    
    // If no match, add left row with nulls for right columns
    if not HasMatch then
    begin
      ResultRow := Result.FRowCount;
      // Copy left frame columns
      for K := 0 to High(FColumns) do
        Result.FColumns[K].Data[ResultRow] := FColumns[K].Data[I];
        
      // Set nulls for right frame columns
      for K := Length(FColumns) to High(Result.FColumns) do
        Result.FColumns[K].Data[ResultRow] := Null;
        
      Inc(Result.FRowCount);
    end;
  end;
  
  // Add unmatched right rows
  for J := 0 to Other.FRowCount - 1 do
    if not MatchedRight[J] then
    begin
      ResultRow := Result.FRowCount;
      // Set nulls for left frame columns
      for K := 0 to High(FColumns) do
        Result.FColumns[K].Data[ResultRow] := Null;
        
      // Copy right frame columns
      for K := 0 to High(Other.FColumns) do
        if not Contains(CommonCols, Other.FColumns[K].Name) then
          Result.FColumns[Length(FColumns) + K].Data[ResultRow] := 
            Other.FColumns[K].Data[J];
            
      Inc(Result.FRowCount);
    end;
end;
end. 
