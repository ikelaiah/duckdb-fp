unit DuckDB.DataFrame;

{$mode objfpc}{$H+}{$J-}

interface

uses
  SysUtils, Classes, Variants, libduckdb, Math, TypInfo, Generics.Collections;

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
    dctDate,       // Date without time
    dctTimestamp,  // Date with time
    dctString,     // Variable-length string
    dctBlob        // Binary large object
  );

  { Column information including metadata and data matching DuckDB's data type}
  TDuckDBColumn = record
    Name: string;           // Column name from query
    DataType: TDuckDBColumnType;  // Column's data type
    Data: array of Variant; // Actual column data
  end;

  { Statistical measures for numeric columns }
  TColumnStats = record
    Count: Integer;         // Total number of rows
    Mean: Double;           // Average value
    StdDev: Double;        // Standard deviation
    Skewness: Double;      // Measure of distribution asymmetry (0 is symmetric)
    Kurtosis: Double;      // Measure of "tailedness" compared to normal distribution
    NonMissingRate: Double; // Percentage of non-null values (1.0 = no nulls)
    Min: Variant;          // Minimum value
    Q1: Double;            // First quartile (25th percentile)
    Median: Double;        // Median (50th percentile)
    Q3: Double;            // Third quartile (75th percentile)
    Max: Variant;          // Maximum value
    NullCount: Integer;    // Number of null values
    Ordered: Boolean;        // Is the data ordered?
    NUnique: Integer;       // Number of unique values
    TopCounts: string;      // Most frequent values and their counts
  end;

  TStringArray = array of string;  // Add this type declaration at the unit level

  { DataFrame class for handling query results in DuckDB compatible datatype}
  TDuckFrame = class
  private
    FColumns: array of TDuckDBColumn;  // Array of columns
    FRowCount: Integer;                // Number of rows in the DataFrame
    
    { Core: Helper functions for data access and calculations }
    function GetColumnCount: Integer;
    function GetColumn(Index: Integer): TDuckDBColumn;
    function GetColumnByName(const Name: string): TDuckDBColumn;
    function GetValue(Row, Col: Integer): Variant;
    function GetValueByName(Row: Integer; const ColName: string): Variant;
    function FindColumnIndex(const Name: string): Integer;
    
    { Stats: Type mapping and statistical calculations }
    function MapDuckDBType(duckdb_type: duckdb_type): TDuckDBColumnType;
    function IsNumericColumn(const Col: TDuckDBColumn): Boolean;
    function CalculateColumnStats(const Col: TDuckDBColumn): TColumnStats;
    function CalculatePercentile(const Values: array of Double; Percentile: Double): Double;

    { Core: Helper functions for union operations }
    function GetCommonColumns(const Other: TDuckFrame): TStringArray;
    function GetAllColumns(const Other: TDuckFrame): TStringArray;
    function TryConvertValue(const Value: Variant; FromType, ToType: TDuckDBColumnType): Variant;
    function HasSameStructure(const Other: TDuckFrame): Boolean;
    function GetColumnNames: TStringArray;
    
  public

    { Constructor and destructor }
    constructor Create;
    destructor Destroy; override;
    
    { Core: DataFrame operations }
    procedure LoadFromResult(AResult: pduckdb_result);  // Load data from DuckDB result
    procedure Clear;                                    // Clear all data
    procedure Print(MaxRows: Integer = 10);            // Print DataFrame contents
    
    { Core: Union operations }
    function Union(const Other: TDuckFrame; Mode: TUnionMode = umStrict): TDuckFrame;
    function UnionAll(const Other: TDuckFrame; Mode: TUnionMode = umStrict): TDuckFrame;
    function Distinct: TDuckFrame;

  { IO: Save to file operations }
    procedure SaveToCSV(const FileName: string);       // Export to CSV file

    { Stats: Data analysis methods }
    function Head(Count: Integer = 5): TDuckFrame;     // Get first N rows
    function Tail(Count: Integer = 5): TDuckFrame;     // Get last N rows
    function Select(const ColumnNames: array of string): TDuckFrame;  // Select columns
    procedure Describe;                                // Show statistical summary
    function NullCount: TDuckFrame;                   // Count null values per column
    procedure Info;                                    // Show DataFrame structure info
    
    { Stat: Missing data handling }
    function DropNA: TDuckFrame;                  // Remove rows with any null values
    function FillNA(const Value: Variant): TDuckFrame;  // Fill null values
    
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


  end;

{ Helper function for sorting }
procedure QuickSort(var A: array of Double; iLo, iHi: Integer);
procedure QuickSortWithIndices(var Values: array of Double; var Indices: array of Integer; Left, Right: Integer);

implementation

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
  I: Integer;
begin
  for I := 0 to Length(FColumns) - 1 do
    SetLength(FColumns[I].Data, 0);
  SetLength(FColumns, 0);
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
    DUCKDB_TYPE_TIME: Result := dctString;
    DUCKDB_TYPE_INTERVAL: Result := dctString;
    DUCKDB_TYPE_VARCHAR: Result := dctString;
    DUCKDB_TYPE_BLOB: Result := dctBlob;
    else Result := dctUnknown;
  end;
end;

procedure TDuckFrame.LoadFromResult(AResult: pduckdb_result);
var
  ColCount, Row, Col: Integer;
  StrValue: PAnsiChar;
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
          FColumns[Col].Data[Row] := Null;
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
      S := VarToStr(FColumns[Col].Data[Row]);
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
        Write(Format('%-*s ', [ColWidths[Col] + 1, VarToStr(FColumns[Col].Data[Row])]));
      WriteLn;
    end;
    
    WriteLn('...');
    
    // Print last half
    StartRow := FRowCount - (MaxRows div 2);
    EndRow := FRowCount - 1;
    for Row := StartRow to EndRow do
    begin
      for Col := 0 to Length(FColumns) - 1 do
        Write(Format('%-*s ', [ColWidths[Col] + 1, VarToStr(FColumns[Col].Data[Row])]));
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
        Write(Format('%-*s ', [ColWidths[Col] + 1, VarToStr(FColumns[Col].Data[Row])]));
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
begin
  // Handle empty or single-value arrays
  N := Length(Values);
  if N = 0 then
    Exit(0);
  if N = 1 then
    Exit(Values[0]);

  // Calculate interpolation position
  Position := Percentile * (N - 1);
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

procedure QuickSort(var A: array of Double; iLo, iHi: Integer);
var
  Lo, Hi: Integer;
  Pivot, T: Double;
begin
  Lo := iLo;
  Hi := iHi;
  Pivot := A[(Lo + Hi) div 2];

  repeat
    while A[Lo] < Pivot do Inc(Lo);
    while A[Hi] > Pivot do Dec(Hi);
    if Lo <= Hi then
    begin
      T := A[Lo];
      A[Lo] := A[Hi];
      A[Hi] := T;
      Inc(Lo);
      Dec(Hi);
    end;
  until Lo > Hi;

  if Hi > iLo then QuickSort(A, iLo, Hi);
  if Lo < iHi then QuickSort(A, Lo, iHi);
end;

// Add this helper function at the unit level (outside the class)
procedure QuickSortWithIndices(var Values: array of Double; var Indices: array of Integer; Left, Right: Integer);
var
  I, J: Integer;
  Pivot, TempValue: Double;
  TempIndex: Integer;
begin
  if Left < Right then
  begin
    I := Left;
    J := Right;
    Pivot := Values[(Left + Right) div 2];
    
    repeat
      while Values[I] < Pivot do Inc(I);
      while Values[J] > Pivot do Dec(J);
      
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
      QuickSortWithIndices(Values, Indices, Left, J);
    if I < Right then
      QuickSortWithIndices(Values, Indices, I, Right);
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
  MAX_WIDTH = 50;  // Maximum width of the histogram in characters
  BAR_CHAR = '#';  // Character to use for the bars
var
  Col: TDuckDBColumn;
  MinVal, MaxVal, BinWidth: Double;
  BinCounts: array of Integer;
  DataPoints: array of Double;
  ValidCount, MaxCount: Integer;
  I, BinIndex: Integer;
  Scale: Double;
  BarWidth: Integer;
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

  if ValidCount = 0 then
  begin
    WriteLn('No valid data to plot histogram');
    Exit;
  end;

  // Initialize bins
  SetLength(BinCounts, Bins);
  BinWidth := (MaxVal - MinVal) / Bins;
  if BinWidth = 0 then BinWidth := 1;  // Handle case where all values are the same

  // Count values in each bin
  MaxCount := 0;
  for I := 0 to ValidCount - 1 do
  begin
    BinIndex := Trunc((DataPoints[I] - MinVal) / BinWidth);
    if BinIndex = Bins then Dec(BinIndex);  // Handle maximum value edge case
    Inc(BinCounts[BinIndex]);
    if BinCounts[BinIndex] > MaxCount then
      MaxCount := BinCounts[BinIndex];
  end;

  // Calculate scale factor for display
  Scale := MAX_WIDTH / MaxCount;

  // Print histogram
  WriteLn;
  WriteLn('Histogram of ', ColumnName);
  WriteLn('Range: ', MinVal:0:2, ' to ', MaxVal:0:2);
  WriteLn('Bin width: ', BinWidth:0:2);
  WriteLn;

  // Print each bin
  for I := 0 to Bins - 1 do
  begin
    // Create bin label
    BinLabel := Format('[%0:.2f-%0:.2f)', 
      [MinVal + I * BinWidth, MinVal + (I + 1) * BinWidth]);
    
    // Calculate bar width
    BarWidth := Round(BinCounts[I] * Scale);
    
    // Print bar
    Write(Format('%-15s |', [BinLabel]));
    Write(StringOfChar(BAR_CHAR, BarWidth));
    WriteLn(Format(' %d', [BinCounts[I]]));
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

function TDuckFrame.TryConvertValue(const Value: Variant; 
  FromType, ToType: TDuckDBColumnType): Variant;
begin
  if VarIsNull(Value) then
    Exit(Null);
    
  try
    case ToType of
      dctBoolean:
        Result := Boolean(Value);
      dctTinyInt, dctSmallInt, dctInteger:
        Result := Integer(Value);
      dctBigInt:
        Result := Int64(Value);
      dctFloat, dctDouble:
        Result := Double(Value);
      dctString:
        Result := VarToStr(Value);
      else
        Result := Value;  // Keep original if no conversion needed
    end;
  except
    Result := Null;  // Return NULL if conversion fails
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

end. 
