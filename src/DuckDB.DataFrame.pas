unit DuckDB.DataFrame;

{$mode objfpc}{$H+}{$J-}
{$modeswitch advancedrecords}
interface

uses
  SysUtils, Classes, Variants, Math, TypInfo, Generics.Collections, DateUtils, DuckDB.Base, libduckdb;

type
  TStringArray = array of string;
  TDuckDBColumnArray = array of TDuckDBColumn;

  { DataFrame class for handling query results in DuckDB compatible datatype }
  TDuckFrame = class(TInterfacedObject, IDuckFrame)
  private
    FColumns: TDuckDBColumnArray;  // Array of columns
    FRowCount: Integer;             // Number of rows in the DataFrame

    { Core: Union-related helper functions for combining dataframes }
    function GetCommonColumns(const Other: IDuckFrame): TStringArray;
    function GetAllColumns(const Other: IDuckFrame): TStringArray;
    function HasSameStructure(const Other: IDuckFrame): Boolean;

    { Stats: Type mapping and statistical calculations }
    function MapDuckDBType(duckdb_type: duckdb_type): TDuckDBColumnType;
    function IsNumericColumn(const Col: TDuckDBColumn): Boolean;

    { Private helpers for constructors }
    procedure InitializeBlank(const AColumnNames: TStringArray; 
                             const AColumnTypes: array of TDuckDBColumnType);

    { Helper method to extract values from DuckDB result }
    function ExtractDuckDBValue(AResult: pduckdb_result; Row, Col: Integer; 
      DataType: TDuckDBColumnType): Variant;

    { Prevent direct access to private fields outside public methods }
    property InternalColumns: TDuckDBColumnArray read FColumns write FColumns;
    property InternalRowCount: Integer read FRowCount write FRowCount;

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

    { Properties }
    property RowCount: Integer read FRowCount;
    property ColumnCount: Integer read GetColumnCount;
    property Columns[Index: Integer]: TDuckDBColumn read GetColumn;
    property ColumnsByName[const Name: string]: TDuckDBColumn read GetColumnByName;
    property Values[Row, Col: Integer]: Variant read GetValue;
    property ValuesByName[Row: Integer; const ColName: string]: Variant read GetValueByName; default;

    { Constructors: More Constructors }
    constructor CreateBlank(const AColumnNames: TStringArray;
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
    procedure SetValue(const ARow: Integer; const AColName: string;
                       const AValue: Variant);

    { DataFrame operations }
    function Filter(const Condition: string): IDuckFrame;
    function Sort(const ColNames: array of string): IDuckFrame;
    function Where(const Condition: string): IDuckFrame;

    { Helper function for sorting }
    procedure QuickSort(var A: array of Double; iLo, iHi: Integer);
    procedure QuickSortWithIndices(var Items: array of Double; var Indices: array of Integer; Left, Right: Integer);
  end;

implementation

{ TDuckFrame }

constructor TDuckFrame.Create;
begin
  inherited Create;
  InitializeBlank([], []);
end;

constructor TDuckFrame.CreateBlank(const AColumnNames: TStringArray;
                                   const AColumnTypes: array of TDuckDBColumnType);
begin
  inherited Create;
  InitializeBlank(AColumnNames, AColumnTypes);
end;

destructor TDuckFrame.Destroy;
begin
  // Clean up resources if necessary
  inherited Destroy;
end;

procedure TDuckFrame.InitializeBlank(const AColumnNames: TStringArray; 
                                     const AColumnTypes: array of TDuckDBColumnType);
var
  I: Integer;
begin
  Clear;

  if Length(AColumnNames) <> Length(AColumnTypes) then
    raise EDuckDBError.Create('Column names and types arrays must have the same length');

  SetLength(FColumns, Length(AColumnNames));
  for I := 0 to High(AColumnNames) do
  begin
    FColumns[I].Name := AColumnNames[I];
    FColumns[I].DataType := AColumnTypes[I];
    SetLength(FColumns[I].Data, 0);
  end;
  FRowCount := 0;
end;


function DuckDBDateToDateTime(const DuckDate: duckdb_date): TDateTime;
var
  DateStruct: duckdb_date_struct;
begin
  DateStruct := duckdb_from_date(DuckDate);
  Result := EncodeDate(DateStruct.year, DateStruct.month, DateStruct.day);
end;

function DuckDBTimeToDateTime(const DuckTime: duckdb_time): TDateTime;
var
  Micros: Int64;
  Hours, Minutes, Seconds, MSecs: Word;
begin
  Micros := Int64(DuckTime) div Int64(1000);
  Hours := Word(Micros div (1000 * 60 * 60));
  Minutes := Word((Micros div (1000 * 60)) mod 60);
  Seconds := Word((Micros div 1000) mod 60);
  MSecs := Word(Micros mod 1000);
  Result := EncodeTime(Hours, Minutes, Seconds, MSecs);
end;

function DuckDBTimestampToDateTime(const DuckTimestamp: duckdb_timestamp): TDateTime;
var
  DatePart: TDateTime;
  TimePart: TDateTime;
  Micros: Int64;
  DateStruct: duckdb_date_struct;
begin
  Micros := Int64(DuckTimestamp) div Int64(1000000);
  DatePart := UnixToDateTime(Micros);
  TimePart := Frac(DatePart);
  Result := DatePart + TimePart;
end;

procedure TDuckFrame.Clear;
var
  I: Integer;
begin
  for I := 0 to GetColumnCount - 1 do
  begin
    SetLength(FColumns[I].Data, 0);
  end;
  SetLength(FColumns, 0);
  FRowCount := 0;
end;

function TDuckFrame.GetColumnCount: Integer;
begin
  Result := Length(FColumns);
end;

function TDuckFrame.GetColumnNames: TStringArray;
var
  I: Integer;
begin
  SetLength(Result, GetColumnCount);
  for I := 0 to High(Result) do
    Result[I] := FColumns[I].Name;
end;

function TDuckFrame.GetColumn(Index: Integer): TDuckDBColumn;
begin
  if (Index < 0) or (Index >= GetColumnCount) then
    raise EDuckDBError.Create('Column index out of range');
  Result := FColumns[Index];
end;

function TDuckFrame.GetColumnByName(const Name: string): TDuckDBColumn;
var
  Index: Integer;
begin
  Index := FindColumnIndex(Name);
  if Index = -1 then
    raise EDuckDBError.CreateFmt('Column "%s" does not exist', [Name]);
  Result := FColumns[Index];
end;

function TDuckFrame.FindColumnIndex(const Name: string): Integer;
var
  I: Integer;
begin
  Result := -1;
  for I := 0 to GetColumnCount - 1 do
    if SameText(FColumns[I].Name, Name) then
    begin
      Result := I;
      Exit;
    end;
end;

function TDuckFrame.GetValue(Row, Col: Integer): Variant;
begin
  if (Row < 0) or (Row >= RowCount) then
    raise EDuckDBError.Create('Row index out of range');
  if (Col < 0) or (Col >= ColumnCount) then
    raise EDuckDBError.Create('Column index out of range');
  Result := FColumns[Col].Data[Row];
end;

function TDuckFrame.GetValueByName(Row: Integer; const ColName: string): Variant;
var
  ColIndex: Integer;
begin
  ColIndex := FindColumnIndex(ColName);
  if ColIndex = -1 then
    raise EDuckDBError.CreateFmt('Column "%s" does not exist', [ColName]);
  if (Row < 0) or (Row >= RowCount) then
    raise EDuckDBError.Create('Row index out of range');
  Result := FColumns[ColIndex].Data[Row];
end;

procedure TDuckFrame.AddColumn(const AName: string; AType: TDuckDBColumnType);
var
  NewIndex: Integer;
  I: Integer;
begin
  // Check for duplicate column name
  if FindColumnIndex(AName) >= 0 then
    raise EDuckDBError.CreateFmt('Column "%s" already exists', [AName]);

  // Add new column
  NewIndex := GetColumnCount;
  SetLength(FColumns, NewIndex + 1);

  FColumns[NewIndex].Name := AName;
  FColumns[NewIndex].DataType := AType;
  SetLength(FColumns[NewIndex].Data, RowCount);

  // Initialize new column with null values
  for I := 0 to RowCount - 1 do
    FColumns[NewIndex].Data[I] := Null;
end;

procedure TDuckFrame.AddRow(const AValues: array of Variant);
var
  ColCount: Integer;
  I: Integer;
begin
  ColCount := GetColumnCount;
  if Length(AValues) <> ColCount then
    raise EDuckDBError.CreateFmt('Expected %d values but got %d', [ColCount, Length(AValues)]);

  // Extend all columns
  for I := 0 to ColCount - 1 do
  begin
    SetLength(FColumns[I].Data, FRowCount + 1);
    FColumns[I].Data[FRowCount] := TryConvertValue(AValues[I], dctUnknown, FColumns[I].DataType);
  end;

  Inc(FRowCount);
end;

function TDuckFrame.TryConvertValue(const Value: Variant; FromType, ToType: TDuckDBColumnType): Variant;
begin
  // Implement conversion logic
  // For simplicity, assuming straightforward conversions
  try
    case ToType of
      dctBoolean:
        Result := Boolean(Value);
      dctTinyInt:
        Result := ShortInt(Value);
      dctSmallInt:
        Result := SmallInt(Value);
      dctInteger:
        Result := Integer(Value);
      dctBigInt:
        Result := Int64(Value);
      dctFloat:
        Result := Single(Value);
      dctDouble:
        Result := Double(Value);
      dctString:
        Result := String(Value);
      dctDate, dctTime, dctTimestamp:
        Result := Value; // Assume already in correct format
      else
        Result := Null;
    end;
  except
    on E: Exception do
      Result := Null; // Return Null on conversion failure
  end;
end;

procedure TDuckFrame.LoadFromResult(AResult: pduckdb_result);
var
  ColCount, RowIdx, ColIdx: Integer;
  ColType: duckdb_type;
  ColName: string;
  Value: Variant;
begin
  if AResult = nil then
    raise EDuckDBError.Create('Invalid DuckDB result');

  Clear;

  // Get column count and initialize structure
  ColCount := duckdb_column_count(AResult);
  SetLength(FColumns, ColCount);

  // Setup columns
  for ColIdx := 0 to ColCount - 1 do
  begin
    ColType := duckdb_column_type(AResult, ColIdx);
    ColName := string(duckdb_column_name(AResult, ColIdx));

    FColumns[ColIdx].Name := ColName;
    FColumns[ColIdx].DataType := MapDuckDBType(ColType);
  end;

  // Get row count and allocate space
  FRowCount := duckdb_row_count(AResult);
  for ColIdx := 0 to ColCount - 1 do
    SetLength(FColumns[ColIdx].Data, FRowCount);

  // Load data
  for RowIdx := 0 to FRowCount - 1 do
  begin
    for ColIdx := 0 to ColCount - 1 do
    begin
      Value := ExtractDuckDBValue(AResult, RowIdx, ColIdx, FColumns[ColIdx].DataType);
      SetValue(RowIdx, FColumns[ColIdx].Name, Value);
    end;
  end;
end;

procedure TDuckFrame.SetValue(const ARow: Integer; const AColName: string; const AValue: Variant);
var
  ColIndex: Integer;
begin
  ColIndex := FindColumnIndex(AColName);
  if ColIndex = -1 then
    raise EDuckDBError.CreateFmt('Column "%s" does not exist', [AColName]);
  if (ARow < 0) or (ARow >= RowCount) then
    raise EDuckDBError.Create('Row index out of range');

  FColumns[ColIndex].Data[ARow] := TryConvertValue(AValue, dctUnknown, FColumns[ColIndex].DataType);
end;

function TDuckFrame.ExtractDuckDBValue(AResult: pduckdb_result; Row, Col: Integer; 
  DataType: TDuckDBColumnType): Variant;
begin
  if duckdb_value_is_null(AResult, Col, Row) then
    Exit(Null);

  case DataType of
    dctBoolean: Result := duckdb_value_boolean(AResult, Col, Row);
    dctTinyInt: Result := duckdb_value_int8(AResult, Col, Row);
    dctSmallInt: Result := duckdb_value_int16(AResult, Col, Row);
    dctInteger: Result := duckdb_value_int32(AResult, Col, Row);
    dctBigInt: Result := duckdb_value_int64(AResult, Col, Row);
    dctFloat: Result := duckdb_value_float(AResult, Col, Row);
    dctDouble: Result := duckdb_value_double(AResult, Col, Row);
    dctString: Result := string(duckdb_value_varchar(AResult, Col, Row));
    dctDate: Result := DuckDBDateToDateTime(duckdb_value_date(AResult, Col, Row));
    dctTime: Result := DuckDBTimeToDateTime(duckdb_value_time(AResult, Col, Row));
    dctTimestamp: Result := DuckDBTimestampToDateTime(duckdb_value_timestamp(AResult, Col, Row));
    else
      Result := Null;
  end;
end;

function TDuckFrame.MapDuckDBType(duckdb_type: duckdb_type): TDuckDBColumnType;
begin
  case duckdb_type of
    DUCKDB_TYPE_BOOLEAN: Result := dctBoolean;
    DUCKDB_TYPE_TINYINT: Result := dctTinyInt;
    DUCKDB_TYPE_SMALLINT: Result := dctSmallInt;
    DUCKDB_TYPE_INTEGER: Result := dctInteger;
    DUCKDB_TYPE_BIGINT: Result := dctBigInt;
    DUCKDB_TYPE_FLOAT: Result := dctFloat;
    DUCKDB_TYPE_DOUBLE: Result := dctDouble;
    DUCKDB_TYPE_VARCHAR: Result := dctString;
    DUCKDB_TYPE_DATE: Result := dctDate;
    DUCKDB_TYPE_TIME: Result := dctTime;
    DUCKDB_TYPE_TIMESTAMP: Result := dctTimestamp;
    else
      Result := dctUnknown;
  end;
end;

function TDuckFrame.Select(const ColumnNames: TStringArray): IDuckFrame;
var
  SelectedFrame: TDuckFrame;
  SelectedTypes: array of TDuckDBColumnType;
  I: Integer;
  J: Integer;
  ColIndex: Integer;
begin
  SelectedFrame := TDuckFrame.Create;
  try
    SetLength(SelectedTypes, Length(ColumnNames));

    // Validate and get types for selected columns
    for I := 0 to High(ColumnNames) do
    begin
      ColIndex := FindColumnIndex(ColumnNames[I]);
      if ColIndex = -1 then
        raise EDuckDBError.CreateFmt('Column "%s" does not exist', [ColumnNames[I]]);
      SelectedTypes[I] := FColumns[ColIndex].DataType;
    end;

    // Initialize the new frame
    SelectedFrame.InitializeBlank(ColumnNames, SelectedTypes);

    // Copy data for selected columns
    for I := 0 to RowCount - 1 do
    begin
      for J := 0 to High(ColumnNames) do
      begin
        SelectedFrame.SetValue(I, ColumnNames[J], GetValueByName(I, ColumnNames[J]));
      end;
    end;

    Result := SelectedFrame;
  except
    SelectedFrame.Free;
    raise;
  end;
end;

function TDuckFrame.Distinct: IDuckFrame;
var
  DB: IDuckDBConnection;
  TempTableName: string;
begin
  Result := TDuckFrame.Create;
  try
    DB := CreateDuckDBConnection;
    try
      DB.Open;

      // Create a unique temporary table name
      TempTableName := Format('temp_distinct_%d', [Random(100000)]);
      DB.WriteToTable(Self, TempTableName);

      try
        // Execute DISTINCT query
        Result := DB.Query(Format('SELECT DISTINCT * FROM %s', [TempTableName]));
      finally
        // Clean up temp table
        DB.ExecuteSQL(Format('DROP TABLE IF EXISTS %s', [TempTableName]));
      end;
    except
      Result := nil;
      raise;
    end;
  except
    Result := nil;
    raise;
  end;
end;

function TDuckFrame.HasSameStructure(const Other: IDuckFrame): Boolean;
var
  I: Integer;
  OtherColumns: TStringArray;
  OtherColTypes: array of TDuckDBColumnType;
begin
  Result := GetColumnCount = Other.GetColumnCount;
  if not Result then
    Exit;

  OtherColumns := Other.GetColumnNames;
  SetLength(OtherColTypes, Length(OtherColumns));

  for I := 0 to High(OtherColumns) do
    OtherColTypes[I] := Other.GetColumn(I).DataType;

  for I := 0 to GetColumnCount - 1 do
  begin
    Result := Result and 
      (FColumns[I].Name = Other.GetColumn(I).Name) and
      (FColumns[I].DataType = Other.GetColumn(I).DataType);
    if not Result then
      Exit;
  end;
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

function TDuckFrame.CorrPearson: IDuckFrame;
var
  DB: IDuckDBConnection;
  TempTableName: string;
  ColNames: TStringArray;
  I: Integer;
  CorrelationQuery: string;
  ColumnList: string;
begin
  Result := TDuckFrame.Create;
  try
    // Get numeric columns only
    ColNames := GetColumnNames;
    ColumnList := '';

    // Build correlation column list
    for I := 0 to High(ColNames) do
    begin
      if IsNumericColumn(GetColumnByName(ColNames[I])) then
      begin
        if ColumnList <> '' then
          ColumnList := ColumnList + ', ';
        ColumnList := ColumnList + ColNames[I];
      end;
    end;

    if ColumnList = '' then
      raise EDuckDBError.Create('No numeric columns found for correlation');

    DB := TDuckDBConnection.Create as IDuckDBConnection;
    try
      DB.Open;

      // Create temporary table
      TempTableName := Format('temp_corr_%d', [Random(100000)]);
      DB.WriteToTable(Self, TempTableName);

      try
        // Build correlation matrix query
        CorrelationQuery := Format(
          'SELECT * FROM (SELECT %s FROM %s) as t, CORR(*)',
          [ColumnList, TempTableName]
        );

        // Execute correlation query
        Result := DB.Query(CorrelationQuery);
      finally
        // Clean up temp table
        DB.ExecuteSQL(Format('DROP TABLE IF EXISTS %s', [TempTableName]));
      end;
    except
      Result := nil;
      raise;
    end;
  except
    Result := nil;
    raise;
  end;
end;

function TDuckFrame.CorrSpearman: IDuckFrame;
var
  NumericCols: array of Integer;
  I, J, K, L: Integer;
  ValidCount: Integer;
  ValuesI, ValuesJ: array of Double;
  IndicesI, IndicesJ: array of Integer;
  RanksI, RanksJ: array of Double;
  SumDiffSq: Double;
  TempRank: Double;
  RankCount: Integer;
  ColNames: TStringArray;
  ColTypes: array of TDuckDBColumnType;
  ValI, ValJ: Variant;
begin
  Result := TDuckFrame.Create;
  try
    // Find numeric columns using public methods
    ColNames := GetColumnNames;
    SetLength(NumericCols, 0);
    for I := 0 to High(ColNames) do
      if IsNumericColumn(GetColumn(I)) then
      begin
        SetLength(NumericCols, Length(NumericCols) + 1);
        NumericCols[High(NumericCols)] := I;
      end;

    // Prepare column names and types for result
    SetLength(ColNames, Length(NumericCols));
    SetLength(ColTypes, Length(NumericCols));
    for I := 0 to High(NumericCols) do
    begin
      ColNames[I] := GetColumn(NumericCols[I]).Name;
      ColTypes[I] := dctDouble;
    end;

    // Initialize result frame
    (Result as TDuckFrame).InitializeBlank(ColNames, ColTypes);

    // Calculate Spearman correlations
    for I := 0 to High(NumericCols) do
    begin
      for J := 0 to High(NumericCols) do
      begin
        // Initialize
        ValidCount := 0;

        // Count valid pairs and collect data
        for K := 0 to RowCount - 1 do
          if not (VarIsNull(GetValue(K, NumericCols[I])) or VarIsNull(GetValue(K, NumericCols[J]))) then
            Inc(ValidCount);

        if ValidCount < 2 then
        begin
          (Result as TDuckFrame).SetValue(I, ColNames[J], Null);
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
        for K := 0 to RowCount - 1 do
          if not (VarIsNull(GetValue(K, NumericCols[I])) or VarIsNull(GetValue(K, NumericCols[J]))) then
          begin
            ValuesI[ValidCount] := GetValue(K, NumericCols[I]);
            ValuesJ[ValidCount] := GetValue(K, NumericCols[J]);
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
        (Result as TDuckFrame).SetValue(I, ColNames[J], 
          1 - (6 * SumDiffSq) / (ValidCount * (Sqr(ValidCount) - 1)));
      end;
    end;
  except
    Result := nil; // Interface will handle cleanup
    raise;
  end;
end;

function TDuckFrame.UniqueCounts(const ColumnName: string): IDuckFrame;
var
  Col: TDuckDBColumn;
  FreqMap: specialize TDictionary<string, Integer>;
  Value: Variant;
  StrValue: string;
  I: Integer;
  CurrentIndex: Integer;
  ColNames: TStringArray;
  ColTypes: array of TDuckDBColumnType;
begin
  Result := TDuckFrame.Create;
  FreqMap := specialize TDictionary<string, Integer>.Create;
  
  try
    // Find the column
    Col := GetColumnByName(ColumnName);
    
    // Count occurrences of each value
    for I := 0 to RowCount - 1 do
    begin
      Value := GetValue(I, FindColumnIndex(ColumnName));
      if not VarIsNull(Value) then
      begin
        StrValue := VarToStr(Value);
        if FreqMap.ContainsKey(StrValue) then
          FreqMap[StrValue] := FreqMap[StrValue] + 1
        else
          FreqMap.Add(StrValue, 1);
      end;
    end;
    
    // Setup column names and types for result
    SetLength(ColNames, 2);
    SetLength(ColTypes, 2);
    
    ColNames[0] := 'Value';
    ColNames[1] := 'Count';
    ColTypes[0] := Col.DataType;
    ColTypes[1] := dctInteger;
    
    // Initialize result frame
    (Result as TDuckFrame).InitializeBlank(ColNames, ColTypes);
    
    // Fill data from frequency map
    CurrentIndex := 0;
    for StrValue in FreqMap.Keys do
    begin
      (Result as TDuckFrame).SetValue(CurrentIndex, 'Value', 
        VarAsType(StrValue, VarType(GetValueByName(0, ColumnName))));
      (Result as TDuckFrame).SetValue(CurrentIndex, 'Count', 
        FreqMap[StrValue]);
      Inc(CurrentIndex);
    end;
  finally
    FreqMap.Free;
  end;
end;

function TDuckFrame.DropNA: IDuckFrame;
var
  Col, Row: Integer;
  KeepRow: array of Boolean;
  NewRowCount: Integer;
  NewRowIndex: Integer;
  ColNames: TStringArray;
  ColTypes: array of TDuckDBColumnType;
  HasNull: Boolean;
begin
  Result := TDuckFrame.Create;
  try
    // Get column information through public methods
    ColNames := GetColumnNames;
    SetLength(ColTypes, Length(ColNames));
    for Col := 0 to High(ColNames) do
      ColTypes[Col] := GetColumn(Col).DataType;
    
    // Initialize array to track which rows to keep
    SetLength(KeepRow, RowCount);
    NewRowCount := 0;
    
    // Mark rows that have no null values
    for Row := 0 to RowCount - 1 do
    begin
      HasNull := False;
      for Col := 0 to High(ColNames) do
      begin
        if VarIsNull(GetValueByName(Row, ColNames[Col])) then
        begin
          HasNull := True;
          Break;
        end;
      end;
      
      KeepRow[Row] := not HasNull;
      if KeepRow[Row] then
        Inc(NewRowCount);
    end;
    
    // Initialize result frame with same structure
    (Result as TDuckFrame).InitializeBlank(ColNames, ColTypes);
    
    // Copy non-null rows
    NewRowIndex := 0;
    for Row := 0 to RowCount - 1 do
    begin
      if KeepRow[Row] then
      begin
        for Col := 0 to High(ColNames) do
        begin
          (Result as TDuckFrame).SetValue(NewRowIndex, ColNames[Col], 
            GetValueByName(Row, ColNames[Col]));
        end;
        Inc(NewRowIndex);
      end;
    end;
  except
    Result := nil; // Interface will handle cleanup
    raise;
  end;
end;

function TDuckFrame.FillNA(const Value: Variant): IDuckFrame;
var
  Col, Row: Integer;
  ColNames: TStringArray;
  ColTypes: array of TDuckDBColumnType;
  CurrentValue: Variant;
begin
  Result := TDuckFrame.Create;
  try
    // Get column information through public methods
    ColNames := GetColumnNames;
    SetLength(ColTypes, Length(ColNames));
    
    for Col := 0 to High(ColNames) do
      ColTypes[Col] := GetColumn(Col).DataType;
    
    // Initialize result frame with same structure
    (Result as TDuckFrame).InitializeBlank(ColNames, ColTypes);
    
    // Copy data and fill nulls using public methods
    for Row := 0 to RowCount - 1 do
    begin
      for Col := 0 to High(ColNames) do
      begin
        CurrentValue := GetValueByName(Row, ColNames[Col]);
        if VarIsNull(CurrentValue) then
          (Result as TDuckFrame).SetValue(Row, ColNames[Col], Value)
        else
          (Result as TDuckFrame).SetValue(Row, ColNames[Col], CurrentValue);
      end;
    end;
  except
    Result := nil; // Interface will handle cleanup
    raise;
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

function TDuckFrame.Head(Count: Integer = 5): IDuckFrame;
var
  Col, Row: Integer;
  RowsToCopy: Integer;
  ColNames: TStringArray;
  ColTypes: array of TDuckDBColumnType;
begin
  Result := TDuckFrame.Create;
  try
    // Get number of rows to copy
    RowsToCopy := Min(Count, RowCount);
    
    // Get column information
    ColNames := GetColumnNames;
    SetLength(ColTypes, Length(ColNames));
    for Col := 0 to High(ColNames) do
      ColTypes[Col] := GetColumn(Col).DataType;
    
    // Initialize the new frame
    TDuckFrame(Result).InitializeBlank(ColNames, ColTypes);
    
    // Copy data
    for Row := 0 to RowsToCopy - 1 do
      for Col := 0 to High(ColNames) do
        Result.ValuesByName[Row, ColNames[Col]] := ValuesByName[Row, ColNames[Col]];
        
  except
    Result := nil;
    raise;
  end;
end;

function TDuckFrame.Tail(Count: Integer = 5): IDuckFrame;
var
  Col, Row, StartRow: Integer;
  RowsToCopy: Integer;
  ColNames: TStringArray;
  ColTypes: array of TDuckDBColumnType;
begin
  Result := TDuckFrame.Create;
  try
    // Calculate rows to copy and starting position
    RowsToCopy := Min(Count, RowCount);
    StartRow := Max(0, RowCount - RowsToCopy);
    
    // Get column information
    ColNames := GetColumnNames;
    SetLength(ColTypes, Length(ColNames));
    for Col := 0 to High(ColNames) do
      ColTypes[Col] := GetColumn(Col).DataType;
    
    // Initialize the new frame
    TDuckFrame(Result).InitializeBlank(ColNames, ColTypes);
    
    // Copy data
    for Row := 0 to RowsToCopy - 1 do
      for Col := 0 to High(ColNames) do
        Result.ValuesByName[Row, ColNames[Col]] := ValuesByName[StartRow + Row, ColNames[Col]];
        
  except
    Result := nil;
    raise;
  end;
end;


function TDuckFrame.Filter(const Condition: string): IDuckFrame;
var
  DB: IDuckDBConnection;
  TempTableName, WhereClause: string;
  FilteredFrame: IDuckFrame;
begin
  DB := TDuckDBConnection.Create as IDuckDBConnection;
  try
    DB.Open;

    // Create a unique temporary table name
    TempTableName := Format('temp_filter_%d', [Random(100000)]);
    DB.WriteToTable(Self, TempTableName);

    try
      // Execute filter using SQL WHERE clause
      FilteredFrame := DB.Query(Format('SELECT * FROM %s WHERE %s', [TempTableName, Condition]));
      Result := FilteredFrame;
    finally
      // Clean up temp table
      DB.ExecuteSQL(Format('DROP TABLE IF EXISTS %s', [TempTableName]));
    end;
  except
    raise;
  end;
end;

function TDuckFrame.Sort(const ColNames: array of string): IDuckFrame;
var
  DB: IDuckDBConnection;
  TempTableName, OrderByClause: string;
  I: Integer;
begin
  if Length(ColNames) = 0 then
    raise EDuckDBError.Create('No columns specified for sorting');

  // Validate ColumnNames
  for I := 0 to High(ColNames) do
    if FindColumnIndex(ColNames[I]) = -1 then
      raise EDuckDBError.CreateFmt('Column "%s" does not exist', [ColNames[I]]);

  Result := TDuckFrame.Create;
  try
    DB := TDuckDBConnection.Create;
    try
      DB.Open;

      // Create a unique temporary table name
      TempTableName := Format('temp_sort_%d', [Random(100000)]);
      DB.WriteToTable(Self, TempTableName);

      // Build ORDER BY clause
      OrderByClause := ColNames[0];
      for I := 1 to High(ColNames) do
        OrderByClause := OrderByClause + ', ' + ColNames[I];

      try
        // Execute sort query
        Result := DB.Query(Format('SELECT * FROM %s ORDER BY %s', [TempTableName, OrderByClause]));
      finally
        // Clean up temp table
        DB.ExecuteSQL(Format('DROP TABLE IF EXISTS %s', [TempTableName]));
      end;
    except
      Result := nil;
      raise;
    end;
  except
    Result := nil;
    raise;
  end;
end;

function TDuckFrame.Where(const Condition: string): IDuckFrame;
begin
  // Where is just an alias for Filter
  Result := Filter(Condition);
end;

end.
