unit DuckDB.DataFrame;

{$mode objfpc}{$H+}{$J-}
{$modeswitch advancedrecords}

interface

uses
  SysUtils, Classes, Variants, libduckdb, Math, TypInfo, Generics.Collections, DateUtils;

type
  EDuckDBError = class(Exception);
  
  TStringArray = array of string;
  
  TDuckDBColumnType = (
    dctBoolean, dctTinyInt, dctSmallInt, dctInteger, dctBigInt,
    dctFloat, dctDouble, dctDate, dctTimestamp, dctString, dctBlob
  );

  TUnionMode = (umStrict, umCommon, umAll);
  
  TColumnStats = record
    Min, Max: Variant;
    Mean, Median, StdDev: Double;
    Q1, Q3: Double;
    NullCount: Integer;
  end;

  { Column definition }
  TDuckDBColumn = record
    Name: string;
    DataType: TDuckDBColumnType;
    Values: TArray<Variant>;
  end;

  { Base connection class }
  TDuckDBConnection = class
  private
    FDatabase: p_duckdb_database;
    FConnection: p_duckdb_connection;
    FDatabasePath: string;
    FIsConnected: Boolean;
    
    procedure CheckError(AState: duckdb_state; const AMessage: string);
    procedure RaiseIfNotConnected;
  protected
    property Database: p_duckdb_database read FDatabase;
    property Connection: p_duckdb_connection read FConnection;
  public
    constructor Create; overload;
    constructor Create(const ADatabasePath: string); overload;
    destructor Destroy; override;
    
    procedure Open(const ADatabasePath: string = '');
    procedure Close;
    function Clone: TDuckDBConnection;
    
    procedure ExecuteSQL(const ASQL: string);
    function Query(const ASQL: string): TDuckFrame;
    function QueryValue(const ASQL: string): Variant;
    
    procedure BeginTransaction;
    procedure Commit;
    procedure Rollback;
    
    property IsConnected: Boolean read FIsConnected;
    property DatabasePath: string read FDatabasePath;
  end;

  { Main DataFrame class }
  TDuckFrame = class
  private
    FConnection: TDuckDBConnection;
    FOwnConnection: Boolean;
    FColumns: array of TDuckDBColumn;
    FRowCount: Integer;
    
    { Core: Union-related helper functions }
    function GetCommonColumns(const Other: TDuckFrame): TStringArray;
    function GetAllColumns(const Other: TDuckFrame): TStringArray;
    function HasSameStructure(const Other: TDuckFrame): Boolean;

    { Stats: Type mapping and calculations }
    function MapDuckDBType(duckdb_type: duckdb_type): TDuckDBColumnType;
    function IsNumericColumn(const Col: TDuckDBColumn): Boolean;

    { Private helpers }
    procedure InitializeBlank(const AColumnNames: array of string; 
                            const AColumnTypes: array of TDuckDBColumnType);
                            
  protected
    procedure LoadFromResult(AResult: pduckdb_result);
    
  public
    { Constructors and destructor }
    constructor Create; overload;
    constructor Create(AConnection: TDuckDBConnection); overload;
    constructor CreateBlank(const AColumnNames: array of string;
                          const AColumnTypes: array of TDuckDBColumnType); overload;
    constructor CreateFromDuckDB(const ADatabase, ATableName: string); overload;
    constructor CreateFromCSV(const AFileName: string; 
                            const AHasHeaders: Boolean = True;
                            const ADelimiter: Char = ','); overload;
    constructor CreateFromParquet(const AFileName: string); overload;
    constructor CreateFromParquet(const Files: array of string); overload;
    destructor Destroy; override;

    { Core: Column-related helper functions }
    function GetColumnCount: Integer;
    function GetColumnNames: TStringArray;
    function GetColumn(Index: Integer): TDuckDBColumn;
    function GetColumnByName(const Name: string): TDuckDBColumn;
    function GetValue(Row, Col: Integer): Variant;
    function GetValueByName(Row: Integer; const ColName: string): Variant;
    function FindColumnIndex(const Name: string): Integer;
    function Select(const ColumnNames: array of string): TDuckFrame;

    { Core: DataFrame operations }
    procedure Clear;
    procedure Print(MaxRows: Integer = 10);
    
    { Core: Value conversion }
    function TryConvertValue(const Value: Variant; FromType, ToType: TDuckDBColumnType): Variant;
    
    { Core: Union operations }
    function Union(const Other: TDuckFrame; Mode: TUnionMode = umStrict): TDuckFrame;
    function UnionAll(const Other: TDuckFrame; Mode: TUnionMode = umStrict): TDuckFrame;
    function Distinct: TDuckFrame;

    { IO operations }
    procedure SaveToCSV(const FileName: string);
    procedure WriteToTable(const TableName: string; const SchemaName: string = 'main');

    { Data Preview }
    function Head(Count: Integer = 5): TDuckFrame;
    function Tail(Count: Integer = 5): TDuckFrame;

    { Data Analysis }
    function CalculateColumnStats(const Col: TDuckDBColumn): TColumnStats;
    function CalculatePercentile(const Values: array of Double; Percentile: Double): Double;
    procedure Describe;
    function NullCount: TDuckFrame;
    procedure Info;
    
    { Data Cleaning }
    function DropNA: TDuckFrame;
    function FillNA(const Value: Variant): TDuckFrame;
    
    { Stats: Advanced analysis }
    function CorrPearson: TDuckFrame;
    function CorrSpearman: TDuckFrame;
    function UniqueCounts(const ColumnName: string): TDuckFrame;
    
    { Plot }
    procedure PlotHistogram(const ColumnName: string; Bins: Integer = 10);
    
    { Manual construction }
    procedure AddColumn(const AName: string; AType: TDuckDBColumnType);
    procedure AddRow(const AValues: array of Variant);
    procedure SetValue(const ARow: Integer; const AColumnName: string; 
                      const AValue: Variant);

    { Properties }
    property RowCount: Integer read FRowCount;
    property ColumnCount: Integer read GetColumnCount;
    property Columns[Index: Integer]: TDuckDBColumn read GetColumn;
    property ColumnsByName[const Name: string]: TDuckDBColumn read GetColumnByName;
    property Values[Row, Col: Integer]: Variant read GetValue;
    property ValuesByName[Row: Integer; const ColName: string]: Variant read GetValueByName; default;
    property Connection: TDuckDBConnection read FConnection;
  end;

// Helper functions
function GetDuckDBTypeString(ColumnType: TDuckDBColumnType): string;
function BoolToString(const Value: Boolean): string;

{ Helper functions }

function GetDuckDBTypeString(ColumnType: TDuckDBColumnType): string;
begin
  case ColumnType of
    dctBoolean: Result := 'BOOLEAN';
    dctTinyInt: Result := 'TINYINT';
    dctSmallInt: Result := 'SMALLINT';
    dctInteger: Result := 'INTEGER';
    dctBigInt: Result := 'BIGINT';
    dctFloat: Result := 'FLOAT';
    dctDouble: Result := 'DOUBLE';
    dctDate: Result := 'DATE';
    dctTimestamp: Result := 'TIMESTAMP';
    dctString: Result := 'VARCHAR';
    dctBlob: Result := 'BLOB';
    else Result := 'VARCHAR';
  end;
end;

function BoolToString(const Value: Boolean): string;
begin
  if Value then
    Result := 'TRUE'
  else
    Result := 'FALSE';
end;

{ TDuckDBConnection }

constructor TDuckDBConnection.Create;
begin
  inherited Create;
  FDatabase := nil;
  FConnection := nil;
  FIsConnected := False;
  FDatabasePath := '';
end;

constructor TDuckDBConnection.Create(const ADatabasePath: string);
begin
  Create;
  Open(ADatabasePath);
end;

destructor TDuckDBConnection.Destroy;
begin
  Close;
  inherited;
end;

procedure TDuckDBConnection.CheckError(AState: duckdb_state; const AMessage: string);
var
  Error: string;
begin
  if AState = DuckDBError then
  begin
    if FConnection <> nil then
      Error := string(duckdb_query_error(FConnection))
    else
      Error := 'Unknown error';
    raise EDuckDBError.CreateFmt('%s: %s', [AMessage, Error]);
  end;
end;

procedure TDuckDBConnection.RaiseIfNotConnected;
begin
  if not FIsConnected then
    raise EDuckDBError.Create('Not connected to database');
end;

procedure TDuckDBConnection.Open(const ADatabasePath: string = '');
begin
  if FIsConnected then
    Close;

  FDatabasePath := ADatabasePath;
  CheckError(
    duckdb_open(PChar(FDatabasePath), @FDatabase),
    'Failed to open database'
  );
  
  CheckError(
    duckdb_connect(FDatabase, @FConnection),
    'Failed to create connection'
  );
  
  FIsConnected := True;
end;

procedure TDuckDBConnection.Close;
begin
  if FConnection <> nil then
  begin
    duckdb_disconnect(@FConnection);
    FConnection := nil;
  end;
  
  if FDatabase <> nil then
  begin
    duckdb_close(@FDatabase);
    FDatabase := nil;
  end;
  
  FIsConnected := False;
end;

function TDuckDBConnection.Clone: TDuckDBConnection;
begin
  Result := TDuckDBConnection.Create;
  if FIsConnected then
  begin
    Result.FDatabasePath := FDatabasePath;
    Result.FDatabase := FDatabase;
    CheckError(
      duckdb_connect(FDatabase, @Result.FConnection),
      'Failed to clone connection'
    );
    Result.FIsConnected := True;
  end;
end;

procedure TDuckDBConnection.ExecuteSQL(const ASQL: string);
var
  Result: duckdb_result;
begin
  RaiseIfNotConnected;
  CheckError(
    duckdb_query(FConnection, PChar(ASQL), @Result),
    'Failed to execute SQL'
  );
  duckdb_destroy_result(@Result);
end;

function TDuckDBConnection.Query(const ASQL: string): TDuckFrame;
var
  DuckResult: duckdb_result;
begin
  RaiseIfNotConnected;
  
  Result := TDuckFrame.Create(Self);
  try
    CheckError(
      duckdb_query(FConnection, PChar(ASQL), @DuckResult),
      'Query failed'
    );
    try
      Result.LoadFromResult(@DuckResult);
    finally
      duckdb_destroy_result(@DuckResult);
    end;
  except
    Result.Free;
    raise;
  end;
end;

function TDuckDBConnection.QueryValue(const ASQL: string): Variant;
var
  DuckResult: duckdb_result;
  StrValue: PAnsiChar;
begin
  RaiseIfNotConnected;
  
  CheckError(
    duckdb_query(FConnection, PChar(ASQL), @DuckResult),
    'Query failed'
  );
  try
    if (duckdb_row_count(@DuckResult) = 0) or
       (duckdb_column_count(@DuckResult) = 0) or
       duckdb_value_is_null(@DuckResult, 0, 0) then
    begin
      Result := Null;
      Exit;
    end;

    case duckdb_column_type(@DuckResult, 0) of
      DUCKDB_TYPE_BOOLEAN:
        Result := duckdb_value_boolean(@DuckResult, 0, 0);
      DUCKDB_TYPE_TINYINT:
        Result := duckdb_value_int8(@DuckResult, 0, 0);
      DUCKDB_TYPE_SMALLINT:
        Result := duckdb_value_int16(@DuckResult, 0, 0);
      DUCKDB_TYPE_INTEGER:
        Result := duckdb_value_int32(@DuckResult, 0, 0);
      DUCKDB_TYPE_BIGINT:
        Result := duckdb_value_int64(@DuckResult, 0, 0);
      DUCKDB_TYPE_FLOAT:
        Result := duckdb_value_float(@DuckResult, 0, 0);
      DUCKDB_TYPE_DOUBLE:
        Result := duckdb_value_double(@DuckResult, 0, 0);
      else
      begin
        StrValue := duckdb_value_varchar(@DuckResult, 0, 0);
        if StrValue <> nil then
        begin
          Result := string(AnsiString(StrValue));
          duckdb_free(StrValue);
        end
        else
          Result := '';
      end;
    end;
  finally
    duckdb_destroy_result(@DuckResult);
  end;
end;

procedure TDuckDBConnection.BeginTransaction;
begin
  RaiseIfNotConnected;
  ExecuteSQL('BEGIN TRANSACTION');
end;

procedure TDuckDBConnection.Commit;
begin
  RaiseIfNotConnected;
  ExecuteSQL('COMMIT');
end;

procedure TDuckDBConnection.Rollback;
begin
  RaiseIfNotConnected;
  ExecuteSQL('ROLLBACK');
end;

{ TDuckFrame }

constructor TDuckFrame.Create;
begin
  inherited Create;
  FConnection := TDuckDBConnection.Create;
  FOwnConnection := True;
  FRowCount := 0;
  SetLength(FColumns, 0);
end;

constructor TDuckFrame.Create(AConnection: TDuckDBConnection);
begin
  inherited Create;
  FConnection := AConnection;
  FOwnConnection := False;
  FRowCount := 0;
  SetLength(FColumns, 0);
end;

destructor TDuckFrame.Destroy;
begin
  Clear;
  if FOwnConnection and (FConnection <> nil) then
    FConnection.Free;
  inherited;
end;

class function TDuckFrame.CreateBlank(const ColumnNames: array of string;
  const ColumnTypes: array of TDuckDBColumnType): TDuckFrame;
var
  I: Integer;
begin
  if Length(ColumnNames) <> Length(ColumnTypes) then
    raise EDuckDBError.Create('Column names and types arrays must have same length');
    
  Result := TDuckFrame.Create;
  try
    SetLength(Result.FColumns, Length(ColumnNames));
    for I := 0 to High(ColumnNames) do
    begin
      Result.FColumns[I].Name := ColumnNames[I];
      Result.FColumns[I].DataType := ColumnTypes[I];
      SetLength(Result.FColumns[I].Values, 0);
    end;
  except
    Result.Free;
    raise;
  end;
end;

class function TDuckFrame.CreateFromCSV(const FileName: string; HasHeaders: Boolean = True): TDuckFrame;
var
  SQL: string;
begin
  Result := TDuckFrame.Create;
  try
    SQL := Format('SELECT * FROM read_csv_auto(''%s''%s)',
      [StringReplace(FileName, '''', '''''', [rfReplaceAll]),
       IfThen(not HasHeaders, ', header=false', '')]);
    
    Result := Result.Connection.Query(SQL);
  except
    Result.Free;
    raise;
  end;
end;

class function TDuckFrame.CreateFromParquet(const FileNames: array of string): TDuckFrame;
var
  SQL: string;
  I: Integer;
begin
  if Length(FileNames) = 0 then
    raise EDuckDBError.Create('No Parquet files specified');
    
  Result := TDuckFrame.Create;
  try
    if Length(FileNames) = 1 then
      SQL := Format('SELECT * FROM read_parquet(''%s'')',
        [StringReplace(FileNames[0], '''', '''''', [rfReplaceAll])])
    else
    begin
      SQL := 'SELECT * FROM (';
      for I := 0 to High(FileNames) do
      begin
        if I > 0 then
          SQL := SQL + ' UNION ALL ';
        SQL := SQL + Format('SELECT * FROM read_parquet(''%s'')',
          [StringReplace(FileNames[I], '''', '''''', [rfReplaceAll])]);
      end;
      SQL := SQL + ')';
    end;
    
    Result := Result.Connection.Query(SQL);
  except
    Result.Free;
    raise;
  end;
end;

procedure TDuckFrame.Clear;
var
  I: Integer;
begin
  for I := 0 to High(FColumns) do
    SetLength(FColumns[I].Values, 0);
  SetLength(FColumns, 0);
  FRowCount := 0;
end;

function TDuckFrame.GetColumnCount: Integer;
begin
  Result := Length(FColumns);
end;

function TDuckFrame.GetRowCount: Integer;
begin
  Result := FRowCount;
end;

function TDuckFrame.GetColumns(Index: Integer): TDuckDBColumn;
begin
  if (Index < 0) or (Index >= Length(FColumns)) then
    raise EDuckDBError.Create('Column index out of bounds');
  Result := FColumns[Index];
end;

procedure TDuckFrame.AddColumn(const Name: string; DataType: TDuckDBColumnType);
var
  NewIndex: Integer;
begin
  NewIndex := Length(FColumns);
  SetLength(FColumns, NewIndex + 1);
  
  FColumns[NewIndex].Name := Name;
  FColumns[NewIndex].DataType := DataType;
  SetLength(FColumns[NewIndex].Values, FRowCount);
  
  // Initialize new column values to NULL
  for var I := 0 to FRowCount - 1 do
    FColumns[NewIndex].Values[I] := Null;
end;

procedure TDuckFrame.AddRow(const Values: array of Variant);
var
  I: Integer;
begin
  if Length(Values) <> Length(FColumns) then
    raise EDuckDBError.Create('Number of values does not match number of columns');
    
  // Extend all columns
  for I := 0 to High(FColumns) do
    SetLength(FColumns[I].Values, FRowCount + 1);
    
  // Add values
  for I := 0 to High(Values) do
    FColumns[I].Values[FRowCount] := Values[I];
    
  Inc(FRowCount);
end;

function TDuckFrame.GetValue(Row, Col: Integer): Variant;
begin
  if (Row < 0) or (Row >= FRowCount) then
    raise EDuckDBError.Create('Row index out of bounds');
  if (Col < 0) or (Col >= Length(FColumns)) then
    raise EDuckDBError.Create('Column index out of bounds');
    
  Result := FColumns[Col].Values[Row];
end;

procedure TDuckFrame.SetValue(Row, Col: Integer; const Value: Variant);
begin
  if (Row < 0) or (Row >= FRowCount) then
    raise EDuckDBError.Create('Row index out of bounds');
  if (Col < 0) or (Col >= Length(FColumns)) then
    raise EDuckDBError.Create('Column index out of bounds');
    
  FColumns[Col].Values[Row] := Value;
end;

function TDuckFrame.GetValueByName(Row: Integer; const ColName: string): Variant;
var
  ColIndex: Integer;
begin
  ColIndex := -1;
  for var I := 0 to High(FColumns) do
    if FColumns[I].Name = ColName then
    begin
      ColIndex := I;
      Break;
    end;
    
  if ColIndex = -1 then
    raise EDuckDBError.CreateFmt('Column "%s" not found', [ColName]);
    
  Result := GetValue(Row, ColIndex);
end;

procedure TDuckFrame.SetValueByName(Row: Integer; const ColName: string; const Value: Variant);
var
  ColIndex: Integer;
begin
  ColIndex := -1;
  for var I := 0 to High(FColumns) do
    if FColumns[I].Name = ColName then
    begin
      ColIndex := I;
      Break;
    end;
    
  if ColIndex = -1 then
    raise EDuckDBError.CreateFmt('Column "%s" not found', [ColName]);
    
  SetValue(Row, ColIndex, Value);
end;

{ DataFrame operations }

function TDuckFrame.Union(const Other: TDuckFrame; Mode: TUnionMode = umStrict): TDuckFrame;
var
  SQL, ColList: string;
  I: Integer;
begin
  if not Assigned(Other) then
    raise EDuckDBError.Create('Other DataFrame is nil');

  case Mode of
    umStrict:
    begin
      if Length(FColumns) <> Length(Other.FColumns) then
        raise EDuckDBError.Create('DataFrames must have same number of columns for strict union');
        
      // Build column list checking types
      ColList := '';
      for I := 0 to High(FColumns) do
      begin
        if I > 0 then ColList := ColList + ', ';
        if (FColumns[I].Name <> Other.FColumns[I].Name) or
           (FColumns[I].DataType <> Other.FColumns[I].DataType) then
          raise EDuckDBError.Create('Column mismatch in strict union');
        ColList := ColList + FColumns[I].Name;
      end;
    end;
    
    umCommon:
    begin
      // Find common columns
      ColList := '';
      for I := 0 to High(FColumns) do
      begin
        if Other.GetValueByName(0, FColumns[I].Name) <> Null then
        begin
          if ColList <> '' then ColList := ColList + ', ';
          ColList := ColList + FColumns[I].Name;
        end;
      end;
      
      if ColList = '' then
        raise EDuckDBError.Create('No common columns found');
    end;
    
    umAll:
    begin
      // Use all columns from both frames
      ColList := '';
      for I := 0 to High(FColumns) do
      begin
        if ColList <> '' then ColList := ColList + ', ';
        ColList := ColList + FColumns[I].Name;
      end;
      
      // Add unique columns from other frame
      for I := 0 to High(Other.FColumns) do
      begin
        if GetValueByName(0, Other.FColumns[I].Name) = Null then
        begin
          if ColList <> '' then ColList := ColList + ', ';
          ColList := ColList + Other.FColumns[I].Name;
        end;
      end;
    end;
  end;

  // Create SQL for union
  SQL := Format('SELECT DISTINCT %s FROM (SELECT %0:s FROM df1 UNION SELECT %0:s FROM df2) t',
    [ColList]);
    
  Result := TDuckFrame.Create(FConnection);
  try
    Result := Result.Connection.Query(SQL);
  except
    Result.Free;
    raise;
  end;
end;

function TDuckFrame.UnionAll(const Other: TDuckFrame; Mode: TUnionMode = umStrict): TDuckFrame;
var
  SQL, ColList: string;
  I: Integer;
begin
  // Similar to Union but without DISTINCT
  // ... [Same initial code as Union] ...
  
  SQL := Format('SELECT %s FROM (SELECT %0:s FROM df1 UNION ALL SELECT %0:s FROM df2) t',
    [ColList]);
    
  Result := TDuckFrame.Create(FConnection);
  try
    Result := Result.Connection.Query(SQL);
  except
    Result.Free;
    raise;
  end;
end;

function TDuckFrame.Distinct: TDuckFrame;
var
  SQL, ColList: string;
  I: Integer;
begin
  ColList := '';
  for I := 0 to High(FColumns) do
  begin
    if I > 0 then ColList := ColList + ', ';
    ColList := ColList + FColumns[I].Name;
  end;
  
  SQL := Format('SELECT DISTINCT %s FROM df', [ColList]);
  
  Result := TDuckFrame.Create(FConnection);
  try
    Result := Result.Connection.Query(SQL);
  except
    Result.Free;
    raise;
  end;
end;

function TDuckFrame.Head(Count: Integer = 5): TDuckFrame;
var
  I, J: Integer;
begin
  Result := TDuckFrame.Create;
  try
    SetLength(Result.FColumns, Length(FColumns));
    for I := 0 to High(FColumns) do
    begin
      Result.FColumns[I].Name := FColumns[I].Name;
      Result.FColumns[I].DataType := FColumns[I].DataType;
      SetLength(Result.FColumns[I].Values, Min(Count, FRowCount));
      
      for J := 0 to Min(Count, FRowCount) - 1 do
        Result.FColumns[I].Values[J] := FColumns[I].Values[J];
    end;
    Result.FRowCount := Min(Count, FRowCount);
  except
    Result.Free;
    raise;
  end;
end;

function TDuckFrame.Tail(Count: Integer = 5): TDuckFrame;
var
  I, J, StartIdx: Integer;
begin
  Result := TDuckFrame.Create;
  try
    SetLength(Result.FColumns, Length(FColumns));
    StartIdx := Max(0, FRowCount - Count);
    
    for I := 0 to High(FColumns) do
    begin
      Result.FColumns[I].Name := FColumns[I].Name;
      Result.FColumns[I].DataType := FColumns[I].DataType;
      SetLength(Result.FColumns[I].Values, FRowCount - StartIdx);
      
      for J := StartIdx to FRowCount - 1 do
        Result.FColumns[I].Values[J - StartIdx] := FColumns[I].Values[J];
    end;
    Result.FRowCount := FRowCount - StartIdx;
  except
    Result.Free;
    raise;
  end;
end;

{ IO Operations }

procedure TDuckFrame.SaveToCSV(const FileName: string);
var
  F: TextFile;
  I, J: Integer;
  Value, QuotedValue: string;
  NeedsQuoting: Boolean;
  
  function ShouldQuoteValue(const S: string): Boolean;
  begin
    Result := (Pos(',', S) > 0) or        // Contains comma
              (Pos('"', S) > 0) or        // Contains quote
              (Pos(#13, S) > 0) or        // Contains CR
              (Pos(#10, S) > 0) or        // Contains LF
              (Pos(' ', S) > 0) or        // Contains space
              (Length(S) > 0) and         // Starts/ends with space
              ((S[1] = ' ') or (S[Length(S)] = ' '));
  end;
  
  function QuoteCSVField(const S: string): string;
  begin
    // Double up any quotes in the string and wrap in quotes
    Result := '"' + StringReplace(S, '"', '""', [rfReplaceAll]) + '"';
  end;

begin
  AssignFile(F, FileName);
  try
    Rewrite(F);
    
    // Write header - Always quote column names as they might contain special characters
    for I := 0 to High(FColumns) do
    begin
      if I > 0 then 
        Write(F, ',');
      Write(F, QuoteCSVField(FColumns[I].Name));
    end;
    WriteLn(F);
    
    // Write data
    for I := 0 to FRowCount - 1 do
    begin
      for J := 0 to High(FColumns) do
      begin
        if J > 0 then 
          Write(F, ',');
          
        if VarIsNull(FColumns[J].Values[I]) then
          Value := ''
        else
          Value := VarToStr(FColumns[J].Values[I]);
        
        // Determine if this value needs quoting
        NeedsQuoting := ShouldQuoteValue(Value);
        
        // Write the properly formatted value
        if NeedsQuoting then
          Write(F, QuoteCSVField(Value))
        else
          Write(F, Value);
      end;
      WriteLn(F);
    end;
  finally
    CloseFile(F);
  end;
end;

procedure TDuckFrame.WriteToTable(const TableName: string; const SchemaName: string = 'main');
var
  SQL, ColDefs: string;
  I: Integer;
begin
  // Build column definitions
  ColDefs := '';
  for I := 0 to High(FColumns) do
  begin
    if I > 0 then ColDefs := ColDefs + ', ';
    ColDefs := ColDefs + Format('%s %s',
      [FColumns[I].Name, GetDuckDBTypeString(FColumns[I].DataType)]);
  end;
  
  // Create table
  SQL := Format('CREATE TABLE %s.%s (%s)',
    [SchemaName, TableName, ColDefs]);
  Connection.ExecuteSQL(SQL);
  
  try
    // Insert data
    Connection.BeginTransaction;
    try
      for I := 0 to FRowCount - 1 do
      begin
        SQL := Format('INSERT INTO %s.%s VALUES (%s)',
          [SchemaName, TableName, BuildInsertValues(I)]);
        Connection.ExecuteSQL(SQL);
      end;
      Connection.Commit;
    except
      Connection.Rollback;
      raise;
    end;
  except
    // If anything goes wrong, try to clean up
    try
      Connection.ExecuteSQL(Format('DROP TABLE IF EXISTS %s.%s',
        [SchemaName, TableName]));
    except
      // Ignore cleanup errors
    end;
    raise;
  end;
end;

{ Display Methods }

procedure TDuckFrame.Print(MaxRows: Integer = 10);
var
  I, J: Integer;
  ColWidths: array of Integer;
  Value: string;
begin
  if Length(FColumns) = 0 then
  begin
    WriteLn('Empty DataFrame');
    Exit;
  end;

  // Calculate column widths
  SetLength(ColWidths, Length(FColumns));
  for I := 0 to High(FColumns) do
  begin
    ColWidths[I] := Length(FColumns[I].Name);
    for J := 0 to FRowCount - 1 do
    begin
      if VarIsNull(FColumns[I].Values[J]) then
        Value := 'NULL'
      else
        Value := VarToStr(FColumns[I].Values[J]);
      ColWidths[I] := Max(ColWidths[I], Length(Value));
    end;
  end;

  // Print header
  for I := 0 to High(FColumns) do
    Write(Format('%-*s ', [ColWidths[I], FColumns[I].Name]));
  WriteLn;

  // Print separator
  for I := 0 to High(FColumns) do
  begin
    for J := 1 to ColWidths[I] do
      Write('-');
    Write(' ');
  end;
  WriteLn;

  // Print data
  for I := 0 to Min(FRowCount - 1, MaxRows - 1) do
  begin
    for J := 0 to High(FColumns) do
    begin
      if VarIsNull(FColumns[J].Values[I]) then
        Value := 'NULL'
      else
        Value := VarToStr(FColumns[J].Values[I]);
      Write(Format('%-*s ', [ColWidths[J], Value]));
    end;
    WriteLn;
  end;

  if FRowCount > MaxRows then
    WriteLn(Format('... (%d more rows)', [FRowCount - MaxRows]));
end;

procedure TDuckFrame.Info;
var
  I: Integer;
  MemSize: Int64;
begin
  WriteLn('DataFrame Information:');
  WriteLn(Format('Rows: %d', [FRowCount]));
  WriteLn(Format('Columns: %d', [Length(FColumns)]));
  WriteLn;
  WriteLn('Columns:');
  for I := 0 to High(FColumns) do
  begin
    WriteLn(Format('  %s: %s',
      [FColumns[I].Name, GetDuckDBTypeString(FColumns[I].DataType)]));
  end;
  
  // Estimate memory usage
  MemSize := 0;
  for I := 0 to High(FColumns) do
    MemSize := MemSize + Length(FColumns[I].Values) * SizeOf(Variant);
  
  WriteLn;
  WriteLn(Format('Estimated memory usage: %d bytes', [MemSize]));
end;

procedure TDuckFrame.Describe;
var
  I: Integer;
  Stats: array of record
    Min, Max: Variant;
    Mean, StdDev: Double;
    NullCount: Integer;
  end;
begin
  SetLength(Stats, Length(FColumns));
  
  // Calculate statistics
  for I := 0 to High(FColumns) do
    CalculateColumnStats(I, Stats[I]);
    
  // Print results
  WriteLn('Statistical Summary:');
  WriteLn;
  
  for I := 0 to High(FColumns) do
  begin
    WriteLn(Format('Column: %s (%s)',
      [FColumns[I].Name, GetDuckDBTypeString(FColumns[I].DataType)]));
    WriteLn(Format('  Count: %d', [FRowCount - Stats[I].NullCount]));
    WriteLn(Format('  Null Count: %d', [Stats[I].NullCount]));
    if not VarIsNull(Stats[I].Min) then
    begin
      WriteLn(Format('  Min: %s', [VarToStr(Stats[I].Min)]));
      WriteLn(Format('  Max: %s', [VarToStr(Stats[I].Max)]));
      WriteLn(Format('  Mean: %.2f', [Stats[I].Mean]));
      WriteLn(Format('  StdDev: %.2f', [Stats[I].StdDev]));
    end;
    WriteLn;
  end;
end;

{ Internal helper methods }

procedure TDuckFrame.LoadFromResult(AResult: pduckdb_result);
var
  ColCount, RowCount, Col, Row: Integer;
  StrValue: PAnsiChar;
begin
  Clear;
  
  ColCount := duckdb_column_count(AResult);
  RowCount := duckdb_row_count(AResult);
  
  SetLength(FColumns, ColCount);
  FRowCount := RowCount;
  
  for Col := 0 to ColCount - 1 do
  begin
    FColumns[Col].Name := string(AnsiString(duckdb_column_name(AResult, Col)));
    FColumns[Col].DataType := MapDuckDBType(duckdb_column_type(AResult, Col));
    SetLength(FColumns[Col].Values, RowCount);
    
    for Row := 0 to RowCount - 1 do
    begin
      if duckdb_value_is_null(AResult, Col, Row) then
        FColumns[Col].Values[Row] := Null
      else
        case FColumns[Col].DataType of
          dctBoolean:
            FColumns[Col].Values[Row] := duckdb_value_boolean(AResult, Col, Row);
          dctTinyInt:
            FColumns[Col].Values[Row] := duckdb_value_int8(AResult, Col, Row);
          dctSmallInt:
            FColumns[Col].Values[Row] := duckdb_value_int16(AResult, Col, Row);
          dctInteger:
            FColumns[Col].Values[Row] := duckdb_value_int32(AResult, Col, Row);
          dctBigInt:
            FColumns[Col].Values[Row] := duckdb_value_int64(AResult, Col, Row);
          dctFloat:
            FColumns[Col].Values[Row] := duckdb_value_float(AResult, Col, Row);
          dctDouble:
            FColumns[Col].Values[Row] := duckdb_value_double(AResult, Col, Row);
          else
          begin
            StrValue := duckdb_value_varchar(AResult, Col, Row);
            if StrValue <> nil then
            begin
              FColumns[Col].Values[Row] := string(AnsiString(StrValue));
              duckdb_free(StrValue);
            end
            else
              FColumns[Col].Values[Row] := '';
          end;
        end;
    end;
  end;
end;

class function TDuckFrame.ReadCSV(const FileName: string; HasHeaders: Boolean = True): TDuckFrame;
var
  SQL: string;
begin
  Result := TDuckFrame.Create;
  try
    // Use DuckDB's CSV reader which handles RFC 4180 correctly
    SQL := Format(
      'SELECT * FROM read_csv_auto(''%s'', ' +
      'header=%s, ' +                     // Handle headers
      'quote=''"'', ' +                   // Use double quote as quote character
      'escape=''"'', ' +                  // Use double quote as escape character
      'delimiter='','', ' +               // Use comma as delimiter
      'null_padding=true)',               // Pad missing values with NULL
      [StringReplace(FileName, '''', '''''', [rfReplaceAll]),
       BoolToString(HasHeaders)]
    );
    
    Result := Result.Connection.Query(SQL);
  except
    Result.Free;
    raise;
  end;
end;

{ Helper functions for statistics }

function CompareDouble(const A, B: Double): Integer;
begin
  if A < B then Result := -1
  else if A > B then Result := 1
  else Result := 0;
end;

function MedianOfSorted(const Values: array of Double): Double;
var
  Mid: Integer;
begin
  if Length(Values) = 0 then
    Result := 0
  else begin
    Mid := Length(Values) div 2;
    if Length(Values) mod 2 = 0 then
      Result := (Values[Mid-1] + Values[Mid]) / 2
    else
      Result := Values[Mid];
  end;
end;

{ TDuckFrame statistical methods }

function TDuckFrame.CalculatePercentile(const Values: array of Double; Percentile: Double): Double;
var
  SortedVals: array of Double;
  N: Integer;
  Position: Double;
  LowIndex, HighIndex: Integer;
  Fraction: Double;
begin
  N := Length(Values);
  if N = 0 then
    Exit(0);
    
  // Copy and sort values
  SetLength(SortedVals, N);
  Move(Values[0], SortedVals[0], N * SizeOf(Double));
  QuickSort(SortedVals, CompareDouble);
  
  // Calculate position
  Position := (N - 1) * (Percentile / 100);
  LowIndex := Trunc(Position);
  Fraction := Frac(Position);
  
  if LowIndex = N - 1 then
    Result := SortedVals[LowIndex]
  else
  begin
    HighIndex := LowIndex + 1;
    Result := SortedVals[LowIndex] + 
              (SortedVals[HighIndex] - SortedVals[LowIndex]) * Fraction;
  end;
end;

function TDuckFrame.CalculateColumnStats(const Col: TDuckDBColumn): TColumnStats;
var
  Values: array of Double;
  ValidCount, I, ValIdx: Integer;
  Sum, SumSq, Mean: Double;
begin
  Result.NullCount := 0;
  
  if not IsNumericColumn(Col) then
  begin
    Result.Min := Null;
    Result.Max := Null;
    Result.Mean := 0;
    Result.Median := 0;
    Result.StdDev := 0;
    Result.Q1 := 0;
    Result.Q3 := 0;
    Exit;
  end;
  
  // First pass: count valid values and find min/max
  ValidCount := 0;
  for I := 0 to High(Col.Values) do
  begin
    if VarIsNull(Col.Values[I]) then
      Inc(Result.NullCount)
    else
    begin
      if ValidCount = 0 then
      begin
        Result.Min := Col.Values[I];
        Result.Max := Col.Values[I];
      end
      else
      begin
        if Col.Values[I] < Result.Min then Result.Min := Col.Values[I];
        if Col.Values[I] > Result.Max then Result.Max := Col.Values[I];
      end;
      Inc(ValidCount);
    end;
  end;
  
  if ValidCount = 0 then
  begin
    Result.Mean := 0;
    Result.Median := 0;
    Result.StdDev := 0;
    Result.Q1 := 0;
    Result.Q3 := 0;
    Exit;
  end;
  
  // Prepare array for percentile calculations
  SetLength(Values, ValidCount);
  ValIdx := 0;
  Sum := 0;
  
  for I := 0 to High(Col.Values) do
  begin
    if not VarIsNull(Col.Values[I]) then
    begin
      Values[ValIdx] := Col.Values[I];
      Sum := Sum + Values[ValIdx];
      Inc(ValIdx);
    end;
  end;
  
  // Calculate mean
  Mean := Sum / ValidCount;
  Result.Mean := Mean;
  
  // Calculate standard deviation
  SumSq := 0;
  for I := 0 to ValidCount - 1 do
    SumSq := SumSq + Sqr(Values[I] - Mean);
  
  if ValidCount > 1 then
    Result.StdDev := Sqrt(SumSq / (ValidCount - 1))
  else
    Result.StdDev := 0;
    
  // Sort for percentiles
  QuickSort(Values, CompareDouble);
  
  // Calculate quartiles
  Result.Median := CalculatePercentile(Values, 50);
  Result.Q1 := CalculatePercentile(Values, 25);
  Result.Q3 := CalculatePercentile(Values, 75);
end;

function TDuckFrame.CorrPearson: TDuckFrame;
var
  I, J, K: Integer;
  ColNames: TStringArray;
  NumericCols: array of Integer;
  N: Integer;
  MeanX, MeanY, SumXY, SumX2, SumY2: Double;
  X, Y: Double;
  Corr: Double;
begin
  // Find numeric columns
  SetLength(NumericCols, 0);
  for I := 0 to High(FColumns) do
    if IsNumericColumn(FColumns[I]) then
    begin
      SetLength(NumericCols, Length(NumericCols) + 1);
      NumericCols[High(NumericCols)] := I;
    end;
    
  // Create result DataFrame
  SetLength(ColNames, Length(NumericCols));
  for I := 0 to High(NumericCols) do
    ColNames[I] := FColumns[NumericCols[I]].Name;
    
  Result := TDuckFrame.CreateBlank(ColNames, 
    Array_Fill(Length(NumericCols), dctDouble));
    
  // Calculate correlations
  for I := 0 to High(NumericCols) do
  begin
    for J := 0 to High(NumericCols) do
    begin
      if I = J then
        Corr := 1.0
      else
      begin
        // Calculate means
        MeanX := 0;
        MeanY := 0;
        N := 0;
        
        for K := 0 to FRowCount - 1 do
        begin
          if not VarIsNull(FColumns[NumericCols[I]].Values[K]) and
             not VarIsNull(FColumns[NumericCols[J]].Values[K]) then
          begin
            MeanX := MeanX + FColumns[NumericCols[I]].Values[K];
            MeanY := MeanY + FColumns[NumericCols[J]].Values[K];
            Inc(N);
          end;
        end;
        
        if N = 0 then
        begin
          Corr := 0;
          Continue;
        end;
        
        MeanX := MeanX / N;
        MeanY := MeanY / N;
        
        // Calculate correlation
        SumXY := 0;
        SumX2 := 0;
        SumY2 := 0;
        
        for K := 0 to FRowCount - 1 do
        begin
          if not VarIsNull(FColumns[NumericCols[I]].Values[K]) and
             not VarIsNull(FColumns[NumericCols[J]].Values[K]) then
          begin
            X := FColumns[NumericCols[I]].Values[K] - MeanX;
            Y := FColumns[NumericCols[J]].Values[K] - MeanY;
            SumXY := SumXY + (X * Y);
            SumX2 := SumX2 + (X * X);
            SumY2 := SumY2 + (Y * Y);
          end;
        end;
        
        if (SumX2 = 0) or (SumY2 = 0) then
          Corr := 0
        else
          Corr := SumXY / Sqrt(SumX2 * SumY2);
      end;
      
      Result.Values[I, J] := Corr;
    end;
  end;
end;

procedure TDuckFrame.PlotHistogram(const ColumnName: string; Bins: Integer = 10);
const
  MaxWidth = 50;  // Maximum width of histogram bars
var
  Col: TDuckDBColumn;
  Min, Max, BinWidth: Double;
  BinCounts: array of Integer;
  MaxCount: Integer;
  I, J, Idx: Integer;
  Value, BarWidth: Integer;
  Stats: TColumnStats;
begin
  Col := GetColumnByName(ColumnName);
  if not IsNumericColumn(Col) then
    raise EDuckDBError.Create('Histogram requires numeric column');
    
  Stats := CalculateColumnStats(Col);
  Min := Stats.Min;
  Max := Stats.Max;
  
  // Initialize bins
  SetLength(BinCounts, Bins);
  BinWidth := (Max - Min) / Bins;
  
  // Count values in each bin
  for I := 0 to FRowCount - 1 do
  begin
    if not VarIsNull(Col.Values[I]) then
    begin
      Idx := Trunc((Col.Values[I] - Min) / BinWidth);
      if Idx = Bins then Dec(Idx);  // Handle maximum value
      Inc(BinCounts[Idx]);
    end;
  end;
  
  // Find maximum count for scaling
  MaxCount := 0;
  for I := 0 to Bins - 1 do
    if BinCounts[I] > MaxCount then
      MaxCount := BinCounts[I];
      
  // Print histogram
  WriteLn('Histogram of ', ColumnName);
  WriteLn;
  
  for I := 0 to Bins - 1 do
  begin
    Write(Format('%0.2f - %0.2f |', 
      [Min + I * BinWidth, Min + (I + 1) * BinWidth]));
      
    BarWidth := Round((BinCounts[I] / MaxCount) * MaxWidth);
    for J := 1 to BarWidth do
      Write('#');
    WriteLn(Format(' %d', [BinCounts[I]]));
  end;
end;

function TDuckFrame.UniqueCounts(const ColumnName: string): TDuckFrame;
var
  Counts: TDictionary<Variant, Integer>;
  Col: TDuckDBColumn;
  I: Integer;
  Value: Variant;
  ValueNames, CountNames: TStringArray;
  Values: array of Variant;
  Counts: array of Integer;
begin
  Col := GetColumnByName(ColumnName);
  Counts := TDictionary<Variant, Integer>.Create;
  try
    // Count occurrences
    for I := 0 to FRowCount - 1 do
    begin
      Value := Col.Values[I];
      if not VarIsNull(Value) then
      begin
        if Counts.ContainsKey(Value) then
          Counts[Value] := Counts[Value] + 1
        else
          Counts.Add(Value, 1);
      end;
    end;

    // Prepare result arrays
    SetLength(Values, Counts.Count);
    SetLength(Counts, Counts.Count);
    I := 0;
    for Value in Counts.Keys do
    begin
      Values[I] := Value;
      Counts[I] := Counts[Value];
      Inc(I);
    end;

    // Create result DataFrame
    Result := TDuckFrame.CreateBlank(
      ['value', 'count'],
      [Col.DataType, dctInteger]
    );

    // Add data
    for I := 0 to High(Values) do
      Result.AddRow([Values[I], Counts[I]]);

  finally
    Counts.Free;
  end;
end;

function TDuckFrame.CorrSpearman: TDuckFrame;
var
  I, J, K: Integer;
  NumericCols: array of Integer;
  RanksX, RanksY: array of Double;
  N: Integer;
  
  function CalculateRanks(const Values: array of Variant; var Ranks: array of Double): Boolean;
  var
    SortedIndices: array of Integer;
    I, J, StartTie, EndTie: Integer;
    CurrentRank: Double;
  begin
    Result := True;
    SetLength(SortedIndices, Length(Values));
    for I := 0 to High(Values) do
      SortedIndices[I] := I;
      
    // Sort indices based on values
    QuickSort(SortedIndices, 
      function(A, B: Integer): Integer
      begin
        if VarIsNull(Values[A]) then Exit(1);
        if VarIsNull(Values[B]) then Exit(-1);
        if Values[A] < Values[B] then Result := -1
        else if Values[A] > Values[B] then Result := 1
        else Result := 0;
      end);
      
    // Assign ranks, handling ties
    I := 0;
    while I < Length(Values) do
    begin
      if VarIsNull(Values[SortedIndices[I]]) then
      begin
        Result := False;
        Break;
      end;
      
      StartTie := I;
      while (I < Length(Values) - 1) and 
            (Values[SortedIndices[I]] = Values[SortedIndices[I + 1]]) do
        Inc(I);
      EndTie := I;
      
      // Average rank for ties
      CurrentRank := (StartTie + EndTie + 2) / 2;
      for J := StartTie to EndTie do
        Ranks[SortedIndices[J]] := CurrentRank;
        
      Inc(I);
    end;
  end;
  
begin
  // Find numeric columns
  SetLength(NumericCols, 0);
  for I := 0 to High(FColumns) do
    if IsNumericColumn(FColumns[I]) then
    begin
      SetLength(NumericCols, Length(NumericCols) + 1);
      NumericCols[High(NumericCols)] := I;
    end;
    
  // Create result DataFrame with same structure as Pearson correlation
  Result := CreateBlank(
    GetColumnNames,
    Array_Fill(Length(FColumns), dctDouble)
  );
  
  SetLength(RanksX, FRowCount);
  SetLength(RanksY, FRowCount);
  
  // Calculate Spearman correlations
  for I := 0 to High(NumericCols) do
  begin
    for J := 0 to High(NumericCols) do
    begin
      if I = J then
        Result.Values[I, J] := 1.0
      else
      begin
        // Calculate ranks for both columns
        if not CalculateRanks(FColumns[NumericCols[I]].Values, RanksX) or
           not CalculateRanks(FColumns[NumericCols[J]].Values, RanksY) then
        begin
          Result.Values[I, J] := Null;
          Continue;
        end;
        
        // Calculate Pearson correlation of ranks
        Result.Values[I, J] := CalculateRankCorrelation(RanksX, RanksY);
      end;
    end;
  end;
end;

function TDuckFrame.DropNA: TDuckFrame;
var
  I, J: Integer;
  KeepRow: Boolean;
  KeepRows: array of Boolean;
  NewRowCount: Integer;
begin
  SetLength(KeepRows, FRowCount);
  NewRowCount := 0;
  
  // Mark rows to keep
  for I := 0 to FRowCount - 1 do
  begin
    KeepRow := True;
    for J := 0 to High(FColumns) do
    begin
      if VarIsNull(FColumns[J].Values[I]) then
      begin
        KeepRow := False;
        Break;
      end;
    end;
    KeepRows[I] := KeepRow;
    if KeepRow then Inc(NewRowCount);
  end;
  
  // Create new DataFrame
  Result := CreateBlank(GetColumnNames, 
    Array_Map(FColumns, function(Col: TDuckDBColumn): TDuckDBColumnType
      begin
        Result := Col.DataType;
      end));
      
  // Copy non-null rows
  for I := 0 to FRowCount - 1 do
  begin
    if KeepRows[I] then
    begin
      Result.AddRow(Array_Map(FColumns, function(Col: TDuckDBColumn): Variant
        begin
          Result := Col.Values[I];
        end));
    end;
  end;
end;

function TDuckFrame.FillNA(const Value: Variant): TDuckFrame;
var
  I, J: Integer;
begin
  Result := Clone;
  for I := 0 to High(Result.FColumns) do
    for J := 0 to Result.FRowCount - 1 do
      if VarIsNull(Result.FColumns[I].Values[J]) then
        Result.FColumns[I].Values[J] := Value;
end;

function TDuckFrame.Clone: TDuckFrame;
var
  I: Integer;
begin
  Result := TDuckFrame.Create(FConnection);
  Result.FOwnConnection := False;
  
  SetLength(Result.FColumns, Length(FColumns));
  Result.FRowCount := FRowCount;
  
  for I := 0 to High(FColumns) do
  begin
    Result.FColumns[I].Name := FColumns[I].Name;
    Result.FColumns[I].DataType := FColumns[I].DataType;
    SetLength(Result.FColumns[I].Values, Length(FColumns[I].Values));
    Move(FColumns[I].Values[0], Result.FColumns[I].Values[0], 
      Length(FColumns[I].Values) * SizeOf(Variant));
  end;
end;

function TDuckFrame.IsNumericColumn(const Col: TDuckDBColumn): Boolean;
begin
  Result := Col.DataType in [
    dctTinyInt, dctSmallInt, dctInteger, dctBigInt, 
    dctFloat, dctDouble
  ];
end;

function TDuckFrame.GetCommonColumns(const Other: TDuckFrame): TStringArray;
var
  I, ResultIdx: Integer;
  OtherNames: TStringArray;
begin
  OtherNames := Other.GetColumnNames;
  SetLength(Result, 0);
  
  for I := 0 to High(FColumns) do
  begin
    if Array_Contains(OtherNames, FColumns[I].Name) then
    begin
      SetLength(Result, Length(Result) + 1);
      Result[High(Result)] := FColumns[I].Name;
    end;
  end;
end;

function TDuckFrame.GetAllColumns(const Other: TDuckFrame): TStringArray;
var
  Names: TDictionary<string, Boolean>;
  I: Integer;
  Name: string;
begin
  Names := TDictionary<string, Boolean>.Create;
  try
    // Add columns from this DataFrame
    for I := 0 to High(FColumns) do
      Names.Add(FColumns[I].Name, True);
      
    // Add unique columns from other DataFrame
    for I := 0 to High(Other.FColumns) do
      if not Names.ContainsKey(Other.FColumns[I].Name) then
        Names.Add(Other.FColumns[I].Name, True);
        
    // Convert to array
    SetLength(Result, Names.Count);
    I := 0;
    for Name in Names.Keys do
    begin
      Result[I] := Name;
      Inc(I);
    end;
  finally
    Names.Free;
  end;
end;

function TDuckFrame.HasSameStructure(const Other: TDuckFrame): Boolean;
var
  I: Integer;
begin
  Result := False;
  
  if Length(FColumns) <> Length(Other.FColumns) then
    Exit;
    
  for I := 0 to High(FColumns) do
  begin
    if (FColumns[I].Name <> Other.FColumns[I].Name) or
       (FColumns[I].DataType <> Other.FColumns[I].DataType) then
      Exit;
  end;
  
  Result := True;
end;

end. 
